/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package shark.execution

import java.util.{HashMap => JavaHashMap, Properties, Map => JavaMap}

import scala.collection.JavaConversions._
import scala.collection.mutable.{ArrayBuffer, Buffer}

import org.apache.hadoop.fs.{Path, PathFilter}
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.{Context, DriverContext}
import org.apache.hadoop.hive.ql.exec.{Task => HiveTask, Utilities}
import org.apache.hadoop.hive.ql.metadata.{Hive, Partition, Table => HiveTable}
import org.apache.hadoop.hive.ql.plan.TableDesc
import org.apache.hadoop.hive.ql.plan.api.StageType
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.Deserializer
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector, StructObjectInspector}
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapred.{FileInputFormat, InputFormat}

import org.apache.spark.SerializableWritable
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import shark.{LogHelper, SharkEnv, Utils}
import shark.execution.serialization.KryoSerializer
import shark.memstore2._
import shark.util.HiveUtils


private[shark]
class SparkLoadWork(
    val databaseName: String,
    val tableName: String,
    val commandType: SparkLoadWork.CommandTypes.Type,
    val preferredStorageLevel: StorageLevel,
    val cacheMode: CacheType.CacheType,
    val unifyView: Boolean)
  extends java.io.Serializable {

  var pathFilterOpt: Option[PathFilter] = None

  var partSpecs: Seq[JavaMap[String, String]] = Nil

  def addPartSpec(partSpec: JavaMap[String, String]) {
    // Not the most efficient, but this method isn't called very often.
    partSpecs = partSpecs ++ Seq(partSpec)
  }
}

object SparkLoadWork {
  object CommandTypes extends Enumeration {
    type Type = Value
    val OVERWRITE, INSERT, NEW_ENTRY = Value
  }

  /**
   * Factory/helper method used in LOAD and INSERT INTO/OVERWRITE analysis. This sets all
   * necessary fields in SparkLoadWork.
   * 
   * A path filter is created if the command is an INSERT and under these conditions:
   * - Table is partitioned, and the partition being updated already exists
   *   (i.e., `partSpecOpt.isDefined == true`)
   * - Table is not partitioned - Hive is used to check for whether it exists.
   */
  def apply(
      db: Hive,
      conf: HiveConf,
      hiveTable: HiveTable,
      partSpecOpt: Option[JavaMap[String, String]],
      commandType: SparkLoadWork.CommandTypes.Type): SparkLoadWork = {
    val cacheMode = CacheType.fromString(hiveTable.getProperty("shark.cache"))
    val preferredStorageLevel = MemoryMetadataManager.getStorageLevelFromString(
      hiveTable.getProperty("shark.cache.storageLevel"))
    val sparkLoadWork = new SparkLoadWork(
      hiveTable.getDbName,
      hiveTable.getTableName,
      commandType,
      preferredStorageLevel,
      cacheMode,
      unifyView = hiveTable.getProperty("shark.cache.unifyView").toBoolean)
    partSpecOpt.foreach(sparkLoadWork.addPartSpec(_))
    if (commandType == SparkLoadWork.CommandTypes.INSERT) {
      if (hiveTable.isPartitioned) {
        partSpecOpt.foreach { partSpec =>
          // None if the partition being updated doesn't exist yet.
          val partitionOpt = Option(db.getPartition(hiveTable, partSpec, false /* forceCreate */))
          sparkLoadWork.pathFilterOpt = partitionOpt.map(part =>
            Utils.createSnapshotFilter(part.getPartitionPath, conf))
        }
      } else {
        sparkLoadWork.pathFilterOpt = Some(Utils.createSnapshotFilter(hiveTable.getPath, conf))
      }
    }
    sparkLoadWork
  }
}

/**
 * A Hive task to load data from disk into the Shark cache. Handles INSERT INTO/OVERWRITE,
 * LOAD INTO/OVERWRITE, CACHE, and CTAS commands.
 */
private[shark]
class SparkLoadTask extends HiveTask[SparkLoadWork] with Serializable with LogHelper {

  override def execute(driveContext: DriverContext): Int = {
    logDebug("Executing " + this.getClass.getName)

    val databaseName = work.databaseName
    val tableName = work.tableName
    val hiveTable = Hive.get(conf).getTable(databaseName, tableName)
    // Used to generate HadoopRDDs.
    val hadoopReader = new HadoopTableReader(Utilities.getTableDesc(hiveTable), conf)
    if (hiveTable.isPartitioned) {
      loadPartitionedTable(
        hiveTable,
        work.partSpecs,
        hadoopReader,
        work.pathFilterOpt)
    } else {
      loadMemoryTable(
        hiveTable,
        hadoopReader,
        work.pathFilterOpt)
    }
    // Success!
    0
  }

  /**
   * Returns a materialized, in-memory RDD comprising TablePartitions backed by columnar stores.
   *
   * @inputRdd A hadoop RDD, or a union of hadoop RDDs if the table is partitioned.
   * @serDeProps Properties used to initialize local ColumnarSerDe instantiations. This contains
   *     the output schema used to create output object inspectors.
   * @storageLevel Storage level for the materialized RDD returned.
   * @broadcasedHiveConf Allows for sharing a Hive Configruation broadcast used to create the Hadoop
   *     `inputRdd`.
   * @inputOI Object inspector used to read rows from `inputRdd`.
   */
  def transformAndMaterializeInput(
      inputRdd: RDD[_],
      serDeProps: Properties,
      storageLevel: StorageLevel,
      broadcastedHiveConf: Broadcast[SerializableWritable[HiveConf]],
      inputOI: StructObjectInspector) = {
    val statsAcc = SharkEnv.sc.accumulableCollection(ArrayBuffer[(Int, TablePartitionStats)]())
    val serializedOI = KryoSerializer.serialize(inputOI)
    val transformedRdd = inputRdd.mapPartitionsWithIndex { case (partIndex, partIter) =>
      val serde = new ColumnarSerDe
      serde.initialize(broadcastedHiveConf.value.value, serDeProps)
      val localInputOI = KryoSerializer.deserialize[ObjectInspector](serializedOI)
      var builder: Writable = null
      partIter.foreach { row =>
        builder = serde.serialize(row.asInstanceOf[AnyRef], localInputOI)
      }
      if (builder == null) {
        // Empty partition.
        statsAcc += Tuple2(partIndex, new TablePartitionStats(Array(), 0))
        Iterator(new TablePartition(0, Array()))
      } else {
        statsAcc += Tuple2(partIndex, builder.asInstanceOf[TablePartitionBuilder].stats)
        Iterator(builder.asInstanceOf[TablePartitionBuilder].build)
      }
    }
    transformedRdd.persist(storageLevel)
    // Run a job to materialize the RDD at the cache `storageLevel` specified.
    transformedRdd.context.runJob(
      transformedRdd, (iter: Iterator[TablePartition]) => iter.foreach(_ => Unit))
    (transformedRdd, statsAcc.value)
  }

  /**
   * Returns Shark MemoryTable that was created or fetched from the metastore, based on the command
   * type handled by this task.
   *
   * @hiveTable Corresponding HiveTable for which to fetch or create the Shark Memorytable.
   */
  def getOrCreateMemoryTable(hiveTable: HiveTable): MemoryTable = {
    val databaseName = hiveTable.getDbName
    val tableName = hiveTable.getTableName
    work.commandType match {
      case SparkLoadWork.CommandTypes.NEW_ENTRY => {
        val newMemoryTable = SharkEnv.memoryMetadataManager.createMemoryTable(
          databaseName,
          tableName,
          work.cacheMode,
          work.preferredStorageLevel,
          work.unifyView)
        // Before setting the table's SerDe property to ColumnarSerDe, record the SerDe used
        // to deserialize rows from disk so that it can be used for successive update operations.
        newMemoryTable.diskSerDe = hiveTable.getDeserializer.getClass.getName
        HiveUtils.alterSerdeInHive(
          databaseName,
          tableName,
          partitionSpecOpt = None,
          classOf[ColumnarSerDe].getName,
          conf)
        newMemoryTable
      }
      case _ => {
        SharkEnv.memoryMetadataManager.getTable(databaseName, tableName) match {
          case Some(table: MemoryTable) => table
          case _ => {
            throw new Exception("Internal error: cached table being updated doesn't exist.")
          }
        }
      }
    }
  }

  /**
   * Handles loading data from disk into the Shark cache for non-partitioned tables.
   *
   * @hiveTable Hive metadata object representing the target table.
   * @hadoopReader Used to create a HadoopRDD from the table's data directory.
   * @pathFilterOpt Defined for INSERT update operations (e.g., INSERT INTO) and passed to
   *     hadoopReader#makeRDDForTable() to determine which new files should be read from the table's
   *     data directory - see the SparkLoadWork#apply() factory method for an example of how the
   *     path filter is created.
   */
  def loadMemoryTable(
      hiveTable: HiveTable,
      hadoopReader: HadoopTableReader,
      pathFilterOpt: Option[PathFilter]) {
    val databaseName = hiveTable.getDbName
    val tableName = hiveTable.getTableName
    val memoryTable = getOrCreateMemoryTable(hiveTable)
    val tableSchema = hiveTable.getSchema
    val serDe = Class.forName(memoryTable.diskSerDe).newInstance.asInstanceOf[Deserializer]
    serDe.initialize(conf, tableSchema)
    // Scan the Hive table's data directory.
    val inputRDD = hadoopReader.makeRDDForTable(
      hiveTable,
      pathFilterOpt,
      serDe.getClass)
    // Transform the HadoopRDD to an RDD[TablePartition].
    val (tablePartitionRDD, tableStats) = transformAndMaterializeInput(
      inputRDD,
      tableSchema,
      memoryTable.preferredStorageLevel,
      hadoopReader.broadcastedHiveConf,
      serDe.getObjectInspector.asInstanceOf[StructObjectInspector])
    memoryTable.tableRDD = work.commandType match {
      case (SparkLoadWork.CommandTypes.OVERWRITE
        | SparkLoadWork.CommandTypes.NEW_ENTRY) => tablePartitionRDD
      case SparkLoadWork.CommandTypes.INSERT => {
        // Union the previous and new RDDs, and their respective table stats.
        val unionedRDD = RDDUtils.unionAndFlatten(tablePartitionRDD, memoryTable.tableRDD)
        SharkEnv.memoryMetadataManager.getStats(databaseName, tableName ) match {
          case Some(previousStatsMap) => unionStatsMaps(tableStats, previousStatsMap)
          case None => Unit
        }
        unionedRDD
      }
    }
    SharkEnv.memoryMetadataManager.putStats(databaseName, tableName, tableStats.toMap)
  }

  /**
   * Returns Shark PartitionedMemorytable that was created or fetched from the metastore, based on
   * the command type handled by this task.
   *
   * @hiveTable Corresponding HiveTable for the returned Shark PartitionedMemorytable.
   */
  def getOrCreatePartitionedTable(
      hiveTable: HiveTable,
      partSpecs: JavaMap[String, String]): PartitionedMemoryTable = {
    val databaseName = hiveTable.getDbName
    val tableName = hiveTable.getTableName
    work.commandType match {
      case SparkLoadWork.CommandTypes.NEW_ENTRY => {
        val newPartitionedTable = SharkEnv.memoryMetadataManager.createPartitionedMemoryTable(
          databaseName,
          tableName,
          work.cacheMode,
          work.preferredStorageLevel,
          work.unifyView,
          hiveTable.getParameters)
        newPartitionedTable.diskSerDe = hiveTable.getDeserializer.getClass.getName
        HiveUtils.alterSerdeInHive(
          databaseName,
          tableName,
          Some(partSpecs),
          classOf[ColumnarSerDe].getName,
          conf)
        newPartitionedTable
      }
      case _ => {
        SharkEnv.memoryMetadataManager.getTable(databaseName, tableName) match {
          case Some(table: PartitionedMemoryTable) => table
          case _ => {
            throw new Exception(
              "Internal error: cached, partitioned table for INSERT handling doesn't exist.")
          }
        }
      }
    }
  }

  /**
   * Handles loading data from disk into the Shark cache for non-partitioned tables.
   *
   * @hiveTable Hive metadata object representing the target table.
   * @partSpecs Sequence of partition key specifications that contains either a single key,
   *     or all of the table's partition keys. This is because only one partition specficiation is
   *     allowed for each append or overwrite command, and new cache entries (i.e, for a CACHE
   *     comand) are full table scans.
   * @hadoopReader Used to create a HadoopRDD from each partition's data directory.
   * @pathFilterOpt Defined for INSERT update operations (e.g., INSERT INTO) and passed to
   *     hadoopReader#makeRDDForTable() to determine which new files should be read from the table
   *     partition's data directory - see the SparkLoadWork#apply() factory method for an example of
   *     how a path filter is created.
   */
  def loadPartitionedTable(
      hiveTable: HiveTable,
      partSpecs: Seq[JavaMap[String, String]],
      hadoopReader: HadoopTableReader,
      pathFilterOpt: Option[PathFilter]) {
    val databaseName = hiveTable.getDbName
    val tableName = hiveTable.getTableName
    val partCols = hiveTable.getPartCols.map(_.getName)

    for (partSpec <- partSpecs) {
      // Read, materialize, and store a columnar-backed RDD for `partSpec`.
      val partitionedTable = getOrCreatePartitionedTable(hiveTable, partSpec)
      val partitionKey = MemoryMetadataManager.makeHivePartitionKeyStr(partCols, partSpec)
      val partition = db.getPartition(hiveTable, partSpec, false /* forceCreate */)

      // Name of the SerDe used to deserialize the partition contents on disk. If the partition
      // specified doesn't currently exist, then default to the table's disk SerDe.
      val partSerDeName = partitionedTable.getDiskSerDe(partitionKey).
        getOrElse(partitionedTable.diskSerDe)
      val partSerDe = Class.forName(partSerDeName).newInstance.asInstanceOf[Deserializer]
      val partSchema = partition.getSchema
      partSerDe.initialize(conf, partSchema)
      // Get a UnionStructObjectInspector that unifies the two StructObjectInspectors for the table
      // columns and the partition columns.
      val unionOI = HiveUtils.makeUnionOIForPartitionedTable(partSchema, partSerDe)
      // Create a HadoopRDD for the file scan.
      val inputRDD = hadoopReader.makeRDDForPartitionedTable(
        Map(partition -> partSerDe.getClass), pathFilterOpt)
      val (tablePartitionRDD, tableStats) = transformAndMaterializeInput(
        inputRDD,
        addPartitionInfoToSerDeProps(partCols, partition.getSchema),
        work.preferredStorageLevel,
        hadoopReader.broadcastedHiveConf,
        unionOI)
      // Determine how to cache the table RDD created.
      val tableOpt = partitionedTable.getPartition(partitionKey)
      if (tableOpt.isDefined && (work.commandType == SparkLoadWork.CommandTypes.INSERT)) {
        val previousRDD = tableOpt.get
        partitionedTable.updatePartition(
          partitionKey, RDDUtils.unionAndFlatten(tablePartitionRDD, previousRDD))
        // Union stats for the previous RDD with the new RDD loaded.
        SharkEnv.memoryMetadataManager.getStats(databaseName, tableName) match {
          case Some(previousStatsMap) => unionStatsMaps(tableStats, previousStatsMap)
          case None => Unit
        }
      } else {
        partitionedTable.putPartition(partitionKey, tablePartitionRDD)
        // If a new partition is added, then the table's SerDe should be used by default.
        partitionedTable.setDiskSerDe(partitionKey, partitionedTable.diskSerDe)
      }
      SharkEnv.memoryMetadataManager.putStats(databaseName, tableName, tableStats.toMap)
    }
  }

  /**
   * Returns a copy of `baseSerDeProps` with row metadata that contains the names and types for the
   * table's partitioning columns.
   */
  def addPartitionInfoToSerDeProps(
    partCols: Seq[String],
    baseSerDeProps: Properties): Properties = {
    val serDeProps = new Properties(baseSerDeProps)
    // Delimited by ","
    var columnNameProperty: String = serDeProps.getProperty(Constants.LIST_COLUMNS)
    // NULL if column types are missing. By default, the SerDeParameters initialized by the
    // ColumnarSerDe will treat all columns as having string types.
    // Delimited by ":"
    var columnTypeProperty: String = serDeProps.getProperty(Constants.LIST_COLUMN_TYPES)

    for (partColName <- partCols) {
      columnNameProperty += "," + partColName
    }
    if (columnTypeProperty != null) {
      for (partColName <- partCols) {
        columnTypeProperty += ":" + Constants.STRING_TYPE_NAME
      }
    }
    serDeProps.setProperty(Constants.LIST_COLUMNS, columnNameProperty)
    serDeProps.setProperty(Constants.LIST_COLUMN_TYPES, columnTypeProperty)
    serDeProps
  }

  def unionStatsMaps(
      targetStatsMap: ArrayBuffer[(Int, TablePartitionStats)],
      otherStatsMap: Iterable[(Int, TablePartitionStats)]
    ): ArrayBuffer[(Int, TablePartitionStats)] = {
    val targetStatsMapSize = targetStatsMap.size
    for ((otherIndex, tableStats) <- otherStatsMap) {
      targetStatsMap.append((otherIndex + targetStatsMapSize, tableStats))
    }
    targetStatsMap
  }

  override def getType = StageType.MAPRED

  override def getName = "MAPRED-LOAD-SPARK"

  override def localizeMRTmpFilesImpl(ctx: Context) = Unit

}
