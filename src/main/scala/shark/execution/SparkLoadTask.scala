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

import java.io.Serializable
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
import shark.parse.QueryBlock
import shark.util.HiveUtils


/**
 * Container for fields needed during SparkLoadTask execution.
 *
 * @param databaseName Namespace for the table being handled.
 * @param tableName Name of the table being handled.
 * @param commandType Type (an enum) of command that will be executed for the target table. See
 *     SparkLoadWork.CommandTypes for a description of which SQL commands correspond to each type.
 * @param preferredStorageLevel Storage level for the RDD loaded into memory.
 * @param cacheMode Cache type that the RDD should be stored in (e.g., Spark heap).
 *     TODO(harvey): Support Tachyon.
 */
private[shark]
class SparkLoadWork(
    val databaseName: String,
    val tableName: String,
    val commandType: SparkLoadWork.CommandTypes.Type,
    val preferredStorageLevel: StorageLevel,
    val cacheMode: CacheType.CacheType)
  extends Serializable {

  // Used for CommandTypes.NEW_ENTRY.
  // True if the table should be stored both on disk and in memory.
  var unifyView: Boolean = _

  // Used for CommandTypes.NEW_ENTRY
  // True if the Shark table entry created should be marked as recoverable. Table properties needed
  // reload the table across sessions will be preserved by the server shutdown hook. See
  // MemoryMetadataManager#processTablesOnShutdown() for shutdown handling and
  // TblProperties#removeSharkProperties() for the properties that are preserved.
  var reloadOnRestart: Boolean = _

  // Defined if the command is an INSERT and under these conditions:
  // - Table is partitioned, and the partition being updated already exists
  //   (i.e., `partSpecOpt.isDefined == true`)
  // - Table is not partitioned - Hive guarantees that data directories exist for updates on such
  //   tables.
  var pathFilterOpt: Option[PathFilter] = None

  // A collection of partition key specifications for partitions to update. Each key is represented
  // by a Map of (partitioning column -> value) pairs.
  var partSpecs: Seq[JavaMap[String, String]] = Nil

  def addPartSpec(partSpec: JavaMap[String, String]) {
    // Not the most efficient, but this method isn't called very often - either a single partition
    // spec is passed for partition update, or all partitions are passed for cache load operation.
    partSpecs = partSpecs ++ Seq(partSpec)
  }
}

object SparkLoadWork {
  object CommandTypes extends Enumeration {
    type Type = Value

    // Type of commands executed by the SparkLoadTask created from a SparkLoadWork.
    // Corresponding SQL commands for each enum:
    // - NEW_ENTRY:
    //   CACHE or ALTER TABLE <table> SET TBLPROPERTIES('shark.cache' = true ... )
    // - INSERT:
    //   INSERT INTO TABLE <table> or LOAD DATA INPATH '...' INTO <table>
    // - OVERWRITE:
    //   INSERT OVERWRITE TABLE <table> or LOAD DATA INPATH '...'' OVERWRITE INTO <table>
    val OVERWRITE, INSERT, NEW_ENTRY = Value
  }

  /**
   * Factory/helper method used in LOAD and INSERT INTO/OVERWRITE analysis. Sets all necessary
   * fields in the SparkLoadWork returned.
   */
  def apply(
      db: Hive,
      conf: HiveConf,
      hiveTable: HiveTable,
      partSpecOpt: Option[JavaMap[String, String]],
      isOverwrite: Boolean): SparkLoadWork = {
    val commandType = if (isOverwrite) {
      SparkLoadWork.CommandTypes.OVERWRITE
    } else {
      SparkLoadWork.CommandTypes.INSERT
    }
    val cacheMode = CacheType.fromString(hiveTable.getProperty("shark.cache"))
    val preferredStorageLevel = MemoryMetadataManager.getStorageLevelFromString(
      hiveTable.getProperty("shark.cache.storageLevel"))
    val sparkLoadWork = new SparkLoadWork(
      hiveTable.getDbName,
      hiveTable.getTableName,
      commandType,
      preferredStorageLevel,
      cacheMode)
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

    // Set the fair scheduler's pool using mapred.fairscheduler.pool if it is defined.
    Option(conf.get("mapred.fairscheduler.pool")).foreach { pool =>
      SharkEnv.sc.setLocalProperty("spark.scheduler.pool", pool)
    }

    val databaseName = work.databaseName
    val tableName = work.tableName
    // Set Spark's job description to be this query.
    SharkEnv.sc.setJobDescription("Updating table %s.%s for a(n) %s"
      .format(databaseName, tableName, work.commandType))
    val hiveTable = Hive.get(conf).getTable(databaseName, tableName)
    // Use HadoopTableReader to help with table scans. The `conf` passed is reused across HadoopRDD
    // instantiations. 
    val hadoopReader = new HadoopTableReader(Utilities.getTableDesc(hiveTable), conf)
    if (hiveTable.isPartitioned) {
      loadPartitionedMemoryTable(
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
   * Returns a materialized, in-memory RDD comprising TablePartitions backed by columnar store.
   *
   * @inputRdd A hadoop RDD, or a union of hadoop RDDs if the table is partitioned.
   * @serDeProps Properties used to initialize local ColumnarSerDe instantiations. This contains the
   *     output schema of the ColumnarSerDe and used to create its output object inspectors.
   * @storageLevel Desired persistance level for the materialized RDD returned.
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
    // Run a job to materialize the RDD, persisted at the `storageLevel` given.
    transformedRdd.context.runJob(
      transformedRdd, (iter: Iterator[TablePartition]) => iter.foreach(_ => Unit))
    (transformedRdd, statsAcc.value)
  }

  /**
   * Returns Shark MemoryTable that was created or fetched from the metastore, based on the command
   * type handled by this task.
   *
   * @hiveTable Corresponding HiveTable for which to fetch or create the returned Shark Memorytable.
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
          work.unifyView,
          work.reloadOnRestart)
        newMemoryTable
      }
      case _ => {
        val tableOpt = SharkEnv.memoryMetadataManager.getTable(databaseName, tableName)
        assert(tableOpt.exists(_.isInstanceOf[MemoryTable]),
          "Memory table being updated cannot be found in the Shark metastore.")
        tableOpt.get.asInstanceOf[MemoryTable]
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
   *     data directory - see the SparkLoadWork#apply() factory method for an example of how a
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
    val serDe = hiveTable.getDeserializer
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
      case (SparkLoadWork.CommandTypes.OVERWRITE | SparkLoadWork.CommandTypes.NEW_ENTRY) =>
        tablePartitionRDD
      case SparkLoadWork.CommandTypes.INSERT => {
        // Union the previous and new RDDs, and their respective table stats.
        val unionedRDD = RDDUtils.unionAndFlatten(tablePartitionRDD, memoryTable.tableRDD)
        SharkEnv.memoryMetadataManager.getStats(databaseName, tableName ) match {
          case Some(previousStatsMap) => SparkLoadTask.unionStatsMaps(tableStats, previousStatsMap)
          case None => Unit
        }
        unionedRDD
      }
    }
    SharkEnv.memoryMetadataManager.putStats(databaseName, tableName, tableStats.toMap)
  }

  /**
   * Returns the created (for CommandType.NEW_ENTRY) or fetched (for CommandType.INSERT or
   * OVERWRITE) PartitionedMemoryTable corresponding to `partSpecs`.
   *
   * @hiveTable Corresponding HiveTable for the Shark PartitionedMemorytable that will be returned.
   * @partSpecs A map of (partitioning column -> corresponding value) that uniquely identifies the
   *     partition being created or updated.
   */
  def getOrCreatePartitionedMemoryTable(
      hiveTable: HiveTable,
      partSpecs: JavaMap[String, String]): PartitionedMemoryTable = {
    val databaseName = hiveTable.getDbName
    val tableName = hiveTable.getTableName
    work.commandType match {
      case SparkLoadWork.CommandTypes.NEW_ENTRY => {
        SharkEnv.memoryMetadataManager.createPartitionedMemoryTable(
          databaseName,
          tableName,
          work.cacheMode,
          work.preferredStorageLevel,
          work.unifyView,
          work.reloadOnRestart,
          hiveTable.getParameters)
      }
      case _ => {
        SharkEnv.memoryMetadataManager.getTable(databaseName, tableName) match {
          case Some(table: PartitionedMemoryTable) => table
          case _ => {
            val tableOpt = SharkEnv.memoryMetadataManager.getTable(databaseName, tableName)
            assert(tableOpt.exists(_.isInstanceOf[PartitionedMemoryTable]),
              "Partitioned memory table being updated cannot be found in the Shark metastore.")
            tableOpt.get.asInstanceOf[PartitionedMemoryTable]
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
  def loadPartitionedMemoryTable(
      hiveTable: HiveTable,
      partSpecs: Seq[JavaMap[String, String]],
      hadoopReader: HadoopTableReader,
      pathFilterOpt: Option[PathFilter]) {
    val databaseName = hiveTable.getDbName
    val tableName = hiveTable.getTableName
    val partCols = hiveTable.getPartCols.map(_.getName)

    for (partSpec <- partSpecs) {
      // Read, materialize, and store a columnar-backed RDD for `partSpec`.
      val partitionedTable = getOrCreatePartitionedMemoryTable(hiveTable, partSpec)
      val partitionKey = MemoryMetadataManager.makeHivePartitionKeyStr(partCols, partSpec)
      val partition = db.getPartition(hiveTable, partSpec, false /* forceCreate */)
      val partSerDe = partition.getDeserializer()
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
        SparkLoadTask.addPartitionInfoToSerDeProps(partCols, partition.getSchema),
        work.preferredStorageLevel,
        hadoopReader.broadcastedHiveConf,
        unionOI)
      // Determine how to cache the table RDD created.
      val tableOpt = partitionedTable.getPartition(partitionKey)
      if (tableOpt.isDefined && (work.commandType == SparkLoadWork.CommandTypes.INSERT)) {
        val previousRDD = tableOpt.get
        partitionedTable.updatePartition(partitionKey,
          RDDUtils.unionAndFlatten(tablePartitionRDD, previousRDD))
        // Union stats for the previous RDD with the new RDD loaded.
        val previousStatsMapOpt = SharkEnv.memoryMetadataManager.getStats(databaseName, tableName)
        assert(SharkEnv.memoryMetadataManager.getStats(databaseName, tableName).isDefined,
          "Stats for %s.%s should be defined for an INSERT operation, but are missing.".
            format(databaseName, tableName))
        SparkLoadTask.unionStatsMaps(tableStats, previousStatsMapOpt.get)
      } else {
        partitionedTable.putPartition(partitionKey, tablePartitionRDD)
      }
      SharkEnv.memoryMetadataManager.putStats(databaseName, tableName, tableStats.toMap)
    }
  }

  override def getType = StageType.MAPRED

  override def getName = "MAPRED-LOAD-SPARK"

  override def localizeMRTmpFilesImpl(ctx: Context) = Unit

}

object SparkLoadTask {

  /**
   * Returns a copy of `baseSerDeProps` with the names and types for the table's partitioning
   * columns appended to respective row metadata properties.
   */
  def addPartitionInfoToSerDeProps(
    partCols: Seq[String],
    baseSerDeProps: Properties): Properties = {
    val serDeProps = new Properties(baseSerDeProps)

    // Column names specified by the Constants.LIST_COLUMNS key are delimited by ",".
    // E.g., for a table created from
    //   CREATE TABLE page_views(key INT, val BIGINT), PARTITIONED BY (dt STRING, country STRING),
    // `columnNameProperties` will be "key,val". We want to append the "dt, country" partition
    // column names to it, and reset the Constants.LIST_COLUMNS entry in the SerDe properties.
    var columnNameProperties: String = serDeProps.getProperty(Constants.LIST_COLUMNS)
    columnNameProperties += "," + partCols.mkString(",")
    serDeProps.setProperty(Constants.LIST_COLUMNS, columnNameProperties)

    // `None` if column types are missing. By default, Hive SerDeParameters initialized by the
    // ColumnarSerDe will treat all columns as having string types.
    // Column types specified by the Constants.LIST_COLUMN_TYPES key are delimited by ":"
    // E.g., for the CREATE TABLE example above, if `columnTypeProperties` is defined, then it
    // will be "int:bigint". Partition columns are strings, so "string:string" should be appended.
    val columnTypePropertiesOpt = Option(serDeProps.getProperty(Constants.LIST_COLUMN_TYPES))
    columnTypePropertiesOpt.foreach { columnTypeProperties =>
      var newColumnTypeProperties = columnTypeProperties +
        (":" + Constants.STRING_TYPE_NAME * partCols.size)
      serDeProps.setProperty(Constants.LIST_COLUMN_TYPES, newColumnTypeProperties)
    }
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
}
