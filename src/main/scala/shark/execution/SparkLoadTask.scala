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
import java.util.{Map => JavaMap, Properties}

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.fs.PathFilter
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.{Context, DriverContext}
import org.apache.hadoop.hive.ql.exec.{Task => HiveTask, Utilities}
import org.apache.hadoop.hive.ql.metadata.{Hive, Table => HiveTable}
import org.apache.hadoop.hive.ql.plan.api.StageType
import org.apache.hadoop.hive.serde.serdeConstants
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector, StructObjectInspector}
import org.apache.hadoop.io.Writable
import org.apache.spark.SerializableWritable
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import shark.{LogHelper, SharkEnv, Utils}
import shark.api.QueryExecutionException
import shark.execution.serialization.KryoSerializer
import shark.memstore2._
import shark.util.HiveUtils

/**
 * Container for fields needed during SparkLoadTask execution.
 *
 * @param databaseName Namespace for the table being handled.
 * @param tableName Name of the table being handled.
 * @param commandType Enum representing the command that will be executed for the target table. See
 *     SparkLoadWork.CommandTypes for a description of which SQL commands correspond to each type.
 * @param cacheMode Cache type that the RDD should be stored in (e.g., Spark heap).
 */
private[shark]
class SparkLoadWork(
    val databaseName: String,
    val tableName: String,
    val commandType: SparkLoadWork.CommandTypes.Type,
    val cacheMode: CacheType.CacheType)
  extends Serializable {

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
    //   CACHE or ALTER TABLE <table> SET TBLPROPERTIES('shark.cache' = `true` ... )
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
    val sparkLoadWork = new SparkLoadWork(
      hiveTable.getDbName,
      hiveTable.getTableName,
      commandType,
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
    SharkEnv.sc.setJobDescription(
      s"Updating table $databaseName.$tableName for a(n) ${work.commandType}")

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
   * Creates and materializes the in-memory, columnar RDD for a given input RDD.
   *
   * @param inputRdd A hadoop RDD, or a union of hadoop RDDs if the table is partitioned.
   * @param serDeProps Properties used to initialize local ColumnarSerDe instantiations. This
   *                   contains the output schema of the ColumnarSerDe and used to create its
   *                   output object inspectors.
   * @param broadcastedHiveConf Allows for sharing a Hive Configuration broadcast used to create
   *                            the Hadoop `inputRdd`.
   * @param inputOI Object inspector used to read rows from `inputRdd`.
   * @param hivePartitionKeyOpt A defined Hive partition key if the RDD being loaded is part of a
   *        Hive-partitioned table.
   */
  private def materialize(
      inputRdd: RDD[_],
      serDeProps: Properties,
      broadcastedHiveConf: Broadcast[SerializableWritable[HiveConf]],
      inputOI: StructObjectInspector,
      tableKey: String,
      hivePartitionKeyOpt: Option[String]) = {
    val statsAcc = SharkEnv.sc.accumulableCollection(ArrayBuffer[(Int, TablePartitionStats)]())
    val offHeapWriter = if (work.cacheMode == CacheType.OFFHEAP) {
      // Find the number of columns in the table schema using `serDeProps`.
      val numColumns = serDeProps.getProperty(serdeConstants.LIST_COLUMNS).split(',').size
      // Use an additional row to store metadata (e.g. number of rows in each partition).
      OffHeapStorageClient.client.createTablePartitionWriter(
        tableKey, hivePartitionKeyOpt, numColumns + 1)
    } else {
      null
    }
    val serializedOI = KryoSerializer.serialize(inputOI)
    var transformedRdd = inputRdd.mapPartitionsWithIndex { case (partIndex, partIter) =>
      val serde = new ColumnarSerDe
      serde.initialize(broadcastedHiveConf.value.value, serDeProps)
      val localInputOI = KryoSerializer.deserialize[ObjectInspector](serializedOI)
      var builder: Writable = null
      partIter.foreach { row =>
        builder = serde.serialize(row.asInstanceOf[AnyRef], localInputOI)
      }
      if (builder == null) {
        // Empty partition.
        statsAcc += Tuple2(partIndex, new TablePartitionStats(Array.empty, 0))
        Iterator(new TablePartition(0, Array()))
      } else {
        statsAcc += Tuple2(partIndex, builder.asInstanceOf[TablePartitionBuilder].stats)
        Iterator(builder.asInstanceOf[TablePartitionBuilder].build())
      }
    }
    // Run a job to materialize the RDD.
    if (work.cacheMode == CacheType.OFFHEAP) {
      // Put the table in off-heap storage.
      logInfo("Putting RDD for %s in off-heap storage".format(tableKey))
      if (work.commandType == SparkLoadWork.CommandTypes.OVERWRITE &&
          OffHeapStorageClient.client.tablePartitionExists(tableKey, hivePartitionKeyOpt)) {
        // For INSERT OVERWRITE, delete the old table or Hive partition directory, if it exists.
        OffHeapStorageClient.client.dropTablePartition(tableKey, hivePartitionKeyOpt)
      }
      offHeapWriter.createTable()
      transformedRdd = transformedRdd.mapPartitionsWithIndex { case(part, iter) =>
        val partition = iter.next()
        partition.toOffHeap.zipWithIndex.foreach { case(buf, column) =>
          offHeapWriter.writeColumnPartition(column, part, buf)
        }
        Iterator(partition)
      }
    } else {
      transformedRdd.persist(StorageLevel.MEMORY_AND_DISK)
    }
    transformedRdd.context.runJob(
      transformedRdd, (iter: Iterator[TablePartition]) => iter.foreach(_ => Unit))
    if (work.cacheMode == CacheType.OFFHEAP) {
      offHeapWriter.setStats(statsAcc.value.toMap)
    }
    (transformedRdd, statsAcc.value)
  }

  /** Returns a MemoryTable for the given Hive table. */
  private def getOrCreateMemoryTable(hiveTable: HiveTable): MemoryTable = {
    val databaseName = hiveTable.getDbName
    val tableName = hiveTable.getTableName
    work.commandType match {
      case SparkLoadWork.CommandTypes.NEW_ENTRY => {
        // This is a new entry, e.g. we are caching a new table or partition.
        // Create a new MemoryTable object and return that.
        SharkEnv.memoryMetadataManager.createMemoryTable(databaseName, tableName, work.cacheMode)
      }
      case _ => {
        // This is an existing entry (e.g. we are handling an INSERT or INSERT OVERWRITE).
        // Get the MemoryTable object from the Shark metastore.
        SharkEnv.memoryMetadataManager.getTable(databaseName, tableName) match {
          case Some(table: MemoryTable) => table
          case None => throw new QueryExecutionException(
            "Memory table being updated cannot be found in the Shark metastore.")
        }
      }
    }
  }

  /**
   * Handles loading data from disk into the Shark cache for non-partitioned tables.
   *
   * @param hiveTable Hive metadata object representing the target table.
   * @param hadoopReader Used to create a HadoopRDD from the table's data directory.
   * @param pathFilterOpt Defined for INSERT update operations (e.g., INSERT INTO) and passed to
   *     hadoopReader#makeRDDForTable() to determine which new files should be read from the table's
   *     data directory - see the SparkLoadWork#apply() factory method for an example of how a
   *     path filter is created.
   */
  private def loadMemoryTable(
      hiveTable: HiveTable,
      hadoopReader: HadoopTableReader,
      pathFilterOpt: Option[PathFilter]) {
    val databaseName = hiveTable.getDbName
    val tableName = hiveTable.getTableName
    val tableSchema = hiveTable.getMetadata
    val serDe = hiveTable.getDeserializer
    serDe.initialize(conf, tableSchema)
    // Scan the Hive table's data directory.
    val inputRDD = hadoopReader.makeRDDForTable(hiveTable, serDe.getClass, pathFilterOpt)
    // Transform the HadoopRDD to an RDD[TablePartition].
    val (tablePartitionRDD, tableStats) = materialize(
      inputRDD,
      tableSchema,
      hadoopReader.broadcastedHiveConf,
      serDe.getObjectInspector.asInstanceOf[StructObjectInspector],
      MemoryMetadataManager.makeTableKey(databaseName, tableName),
      hivePartitionKeyOpt = None)
    if (work.cacheMode != CacheType.OFFHEAP) {
      val memoryTable = getOrCreateMemoryTable(hiveTable)
      work.commandType match {
        case (SparkLoadWork.CommandTypes.OVERWRITE | SparkLoadWork.CommandTypes.NEW_ENTRY) =>
          memoryTable.put(tablePartitionRDD, tableStats.toMap)
        case SparkLoadWork.CommandTypes.INSERT => {
          memoryTable.update(tablePartitionRDD, tableStats)
        }
      }
    }
  }

  /**
   * Returns the created (for CommandType.NEW_ENTRY) or fetched (for CommandType.INSERT or
   * OVERWRITE) PartitionedMemoryTable corresponding to `partSpecs`.
   *
   * @param hiveTable The Hive Table.
   * @param partSpecs A map of (partitioning column -> corresponding value) that uniquely
   *                  identifies the partition being created or updated.
   */
  private def getOrCreatePartitionedMemoryTable(
      hiveTable: HiveTable,
      partSpecs: JavaMap[String, String]): PartitionedMemoryTable = {
    val databaseName = hiveTable.getDbName
    val tableName = hiveTable.getTableName
    work.commandType match {
      case SparkLoadWork.CommandTypes.NEW_ENTRY => {
        SharkEnv.memoryMetadataManager.getTable(databaseName, tableName) match {
          case Some(table: PartitionedMemoryTable) => table
          case None => {
            SharkEnv.memoryMetadataManager.createPartitionedMemoryTable(
              databaseName,
              tableName,
              work.cacheMode,
              hiveTable.getParameters)
          }
        }
      }
      case _ => {
        SharkEnv.memoryMetadataManager.getTable(databaseName, tableName) match {
          case Some(table: PartitionedMemoryTable) => table
          case None => throw new QueryExecutionException(
            "Partitioned memory table being updated cannot be found in the Shark metastore.")
        }
      }
    }
  }

  /**
   * Handles loading data from disk into the Shark cache for non-partitioned tables.
   *
   * @param hiveTable Hive metadata object representing the target table.
   * @param partSpecs Sequence of partition key specifications that contains either a single key,
   *     or all of the table's partition keys. This is because only one partition specficiation is
   *     allowed for each append or overwrite command, and new cache entries (i.e, for a CACHE
   *     comand) are full table scans.
   * @param hadoopReader Used to create a HadoopRDD from each partition's data directory.
   * @param pathFilterOpt Defined for INSERT update operations (e.g., INSERT INTO) and passed to
   *     hadoopReader#makeRDDForTable() to determine which new files should be read from the table
   *     partition's data directory - see the SparkLoadWork#apply() factory method for an example of
   *     how a path filter is created.
   */
  private def loadPartitionedMemoryTable(
      hiveTable: HiveTable,
      partSpecs: Seq[JavaMap[String, String]],
      hadoopReader: HadoopTableReader,
      pathFilterOpt: Option[PathFilter]) {
    val databaseName = hiveTable.getDbName
    val tableName = hiveTable.getTableName
    val partCols = hiveTable.getPartCols.map(_.getName)

    for (partSpec <- partSpecs) {
      // Read, materialize, and store a columnar-backed RDD for `partSpec`.
      val partitionKey = MemoryMetadataManager.makeHivePartitionKeyStr(partCols, partSpec)
      val partition = db.getPartition(hiveTable, partSpec, false /* forceCreate */)
      val tableDesc = Utilities.getTableDesc(hiveTable)
      val tableSerDe = tableDesc.getDeserializerClass().newInstance()
      tableSerDe.initialize(conf, tableDesc.getProperties())
      // Get a UnionStructObjectInspector that unifies the two StructObjectInspectors for the table
      // columns and the partition columns.
      val unionOI = HiveUtils.makeUnionOIForPartitionedTable(partition.getSchema, tableSerDe)
      // Create a HadoopRDD for the file scan.
      val inputRDD = hadoopReader.makeRDDForPartitionedTable(
        Map(partition -> partition.getDeserializer.getClass), pathFilterOpt)
      val (tablePartitionRDD, tableStats) = materialize(
        inputRDD,
        SparkLoadTask.addPartitionInfoToSerDeProps(partCols, partition.getSchema),
        hadoopReader.broadcastedHiveConf,
        unionOI,
        MemoryMetadataManager.makeTableKey(databaseName, tableName),
        Some(partitionKey))
      if (work.cacheMode != CacheType.OFFHEAP) {
        // Handle appends or overwrites.
        val partitionedTable = getOrCreatePartitionedMemoryTable(hiveTable, partSpec)
        if (partitionedTable.containsPartition(partitionKey) &&
            (work.commandType == SparkLoadWork.CommandTypes.INSERT)) {
          partitionedTable.updatePartition(partitionKey, tablePartitionRDD, tableStats)
        } else {
          partitionedTable.putPartition(partitionKey, tablePartitionRDD, tableStats.toMap)
        }
      }
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
  private def addPartitionInfoToSerDeProps(
      partCols: Seq[String],
      baseSerDeProps: Properties): Properties = {
    val serDeProps = new Properties(baseSerDeProps)

    // Column names specified by the Constants.LIST_COLUMNS key are delimited by ",".
    // E.g., for a table created from
    //   CREATE TABLE page_views(key INT, val BIGINT), PARTITIONED BY (dt STRING, country STRING),
    // `columnNameProperties` will be "key,val". We want to append the "dt, country" partition
    // column names to it, and reset the Constants.LIST_COLUMNS entry in the SerDe properties.
    var columnNameProperties: String = serDeProps.getProperty(serdeConstants.LIST_COLUMNS)
    columnNameProperties += "," + partCols.mkString(",")
    serDeProps.setProperty(serdeConstants.LIST_COLUMNS, columnNameProperties)

    // `None` if column types are missing. By default, Hive SerDeParameters initialized by the
    // ColumnarSerDe will treat all columns as having string types.
    // Column types specified by the Constants.LIST_COLUMN_TYPES key are delimited by ":"
    // E.g., for the CREATE TABLE example above, if `columnTypeProperties` is defined, then it
    // will be "int:bigint". Partition columns are strings, so "string:string" should be appended.
    val columnTypePropertiesOpt = Option(serDeProps.getProperty(serdeConstants.LIST_COLUMN_TYPES))
    columnTypePropertiesOpt.foreach { columnTypeProperties =>
      serDeProps.setProperty(serdeConstants.LIST_COLUMN_TYPES,
        columnTypeProperties + ((":" + serdeConstants.STRING_TYPE_NAME) * partCols.size))
    }
    serDeProps
  }
}
