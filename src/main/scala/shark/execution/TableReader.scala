/*
 * Copyright (C) 2012 The Regents of The University California.
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package shark.execution

import java.io.Serializable

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path, PathFilter}
import org.apache.hadoop.mapred.{FileInputFormat, InputFormat, JobConf}
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.api.Constants.META_TABLE_PARTITION_COLUMNS
import org.apache.hadoop.hive.ql.exec.Utilities
import org.apache.hadoop.hive.ql.metadata.{Partition, Table => HiveTable}
import org.apache.hadoop.hive.ql.plan.{PartitionDesc, TableDesc}
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapred.JobConf

import org.apache.spark.rdd.{EmptyRDD, HadoopRDD, RDD, UnionRDD}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.SerializableWritable

import shark.api.QueryExecutionException
import shark.execution.optimization.ColumnPruner
import shark.execution.serialization.JavaSerializer
import shark.{LogHelper, SharkConfVars, SharkEnv, Utils}
import shark.memstore2.{MemoryMetadataManager, TablePartition, TablePartitionStats}
import shark.tachyon.TachyonException


/**
 * A trait for subclasses that handle table scans. In Shark, there is one subclass for each
 * type of table storage: HeapTableReader for Shark tables in Spark's block manager,
 * TachyonTableReader for tables in Tachyon, and HadoopTableReader for Hive tables in a filesystem.
 */
trait TableReader extends LogHelper{

  def makeRDDForTable(hiveTable: HiveTable): RDD[_]

  def makeRDDForTablePartitions(partitions: Seq[Partition]): RDD[_]

}

class TachyonTableReader(@transient _tableDesc: TableDesc) extends TableReader {

  // Split from 'databaseName.tableName'
  private val _tableNameSplit = _tableDesc.getTableName.split('.')
  private val _databaseName = _tableNameSplit(0)
  private val _tableName = _tableNameSplit(1)

  override def makeRDDForTable(hiveTable: HiveTable): RDD[_] = {
    // Table is in Tachyon.
    val tableKey = SharkEnv.makeTachyonTableKey(_databaseName, _tableName)
    if (!SharkEnv.tachyonUtil.tableExists(tableKey)) {
      throw new TachyonException("Table " + tableKey + " does not exist in Tachyon")
    }
    logInfo("Loading table " + tableKey + " from Tachyon.")

    var indexToStats: collection.Map[Int, TablePartitionStats] =
      SharkEnv.memoryMetadataManager.getStats(_databaseName, _tableName).getOrElse(null)

    if (indexToStats == null) {
      val statsByteBuffer = SharkEnv.tachyonUtil.getTableMetadata(tableKey)
      indexToStats = JavaSerializer.deserialize[collection.Map[Int, TablePartitionStats]](
        statsByteBuffer.array())
      logInfo("Loading table " + tableKey + " stats from Tachyon.")
      SharkEnv.memoryMetadataManager.putStats(_databaseName, _tableName, indexToStats)
    }
    SharkEnv.tachyonUtil.createRDD(tableKey)
  }

  override def makeRDDForTablePartitions(partitions: Seq[Partition]): RDD[_] = {
    throw new UnsupportedOperationException("Partitioned tables are not yet supported for Tachyon.")
  }

}

class HeapTableReader(@transient _tableDesc: TableDesc) extends TableReader {

  // Split from 'databaseName.tableName'
  private val _tableNameSplit = _tableDesc.getTableName.split('.')
  private val _databaseName = _tableNameSplit(0)
  private val _tableName = _tableNameSplit(1)

  override def makeRDDForTable(hiveTable: HiveTable): RDD[_] = {
    logInfo("Loading table %s.%s from Spark block manager".format(_databaseName, _tableName))
    val tableOpt = SharkEnv.memoryMetadataManager.getMemoryTable(_databaseName, _tableName)
    if (tableOpt.isEmpty) throwMissingTableException()

    val table = tableOpt.get
    table.tableRDD
  }

  /**
   * Fetch an RDD from the Shark metastore using each partition key given, and return a union of all
   * the fetched RDDs.
   *
   * @param tableKey Name of the partitioned table.
   * @param partitions A collection of Hive-partition metadata, such as partition columns and
   *     partition key specifications.
   */
  override def makeRDDForTablePartitions(partitions: Seq[Partition]): RDD[_] = {
    val hivePartitionRDDs = partitions.map { partition =>
      val partDesc = Utilities.getPartitionDesc(partition)
      // Get partition field info
      val partSpec = partDesc.getPartSpec()
      val partProps = partDesc.getProperties()

      val partColsDelimited = partProps.getProperty(META_TABLE_PARTITION_COLUMNS)
      // Partitioning columns are delimited by "/"
      val partCols = partColsDelimited.trim().split("/").toSeq
      // 'partValues[i]' contains the value for the partitioning column at 'partCols[i]'.
      val partValues = if (partSpec == null) {
        Array.fill(partCols.size)(new String)
      } else {
        partCols.map(col => new String(partSpec.get(col))).toArray
      }

      val partitionKeyStr = MemoryMetadataManager.makeHivePartitionKeyStr(partCols, partSpec)
      val hivePartitionedTableOpt = SharkEnv.memoryMetadataManager.getPartitionedTable(
        _databaseName, _tableName)
      if (hivePartitionedTableOpt.isEmpty) throwMissingTableException()
      val hivePartitionedTable = hivePartitionedTableOpt.get

      val hivePartitionRDDOpt = hivePartitionedTable.getPartition(partitionKeyStr)
      if (hivePartitionRDDOpt.isEmpty) throwMissingPartitionException(partitionKeyStr)
      val hivePartitionRDD = hivePartitionRDDOpt.get

      hivePartitionRDD.mapPartitions { iter =>
        if (iter.hasNext) {
          // Map each tuple to a row object
          val rowWithPartArr = new Array[Object](2)
          val tablePartition = iter.next.asInstanceOf[TablePartition]
          tablePartition.iterator.map { value =>
            rowWithPartArr.update(0, value.asInstanceOf[Object])
            rowWithPartArr.update(1, partValues)
            rowWithPartArr.asInstanceOf[Object]
          }
        } else {
         Iterator()
        }
      }
    }
    if (hivePartitionRDDs.size > 0) {
      new UnionRDD(hivePartitionRDDs.head.context, hivePartitionRDDs)
    } else {
      new EmptyRDD[Object](SharkEnv.sc)
    }
  }

  private def throwMissingTableException() {
    logError("""|Table %s.%s not found in block manager.
                |Are you trying to access a cached table from a Shark session other than the one
                |in which it was created?""".stripMargin.format(_databaseName, _tableName))
    throw new QueryExecutionException("Cached table not found")
  }

  private def throwMissingPartitionException(partValues: String) {
    logError("""|Partition %s for table %s.%s not found in block manager.
                |Are you trying to access a cached table from a Shark session other than the one in
                |which it was created?""".stripMargin.format(partValues, _databaseName, _tableName))
    throw new QueryExecutionException("Cached table partition not found")
  }

}

class HadoopTableReader(@transient _tableDesc: TableDesc, @transient _localHConf: HiveConf)
  extends TableReader {

  // Choose the minimum number of splits. If mapred.map.tasks is set, then use that unless
  // it is smaller than what Spark suggests.
  private val _minSplitsPerRDD = math.max(
    _localHConf.getInt("mapred.map.tasks", 1), SharkEnv.sc.defaultMinSplits)

  HadoopTableReader.addCredentialsToConf(_localHConf)
  private val _broadcastedHiveConf = SharkEnv.sc.broadcast(new SerializableWritable(_localHConf))

  def broadcastedHiveConf = _broadcastedHiveConf

  override def makeRDDForTable(hiveTable: HiveTable): RDD[_] =
    makeRDDForTable(hiveTable, filterOpt = None)
  
  /**
   * Creates a Hadoop RDD to read data from the target table's data directory. Returns a transformed
   * RDD that contains deserialized rows.
   */
  def makeRDDForTable(hiveTable: HiveTable, filterOpt: Option[PathFilter] = None): RDD[_] = {
    assert(!hiveTable.isPartitioned, """makeRDDForTable() cannot be called on a partitioned table,
      since input formats may differ across partitions. Use makeRDDForTablePartitions() instead.""")

    // Create local references to member variables, so that the entire `this` object won't be
    // serialized in the closure below.
    val tableDesc = _tableDesc
    val broadcastedHiveConf = _broadcastedHiveConf

    val tablePath = hiveTable.getPath
    val inputPathStr = applyFilterIfNeeded(tablePath, filterOpt)

    logDebug("Table input: %s".format(tablePath))
    val ifc = hiveTable.getInputFormatClass
      .asInstanceOf[java.lang.Class[InputFormat[Writable, Writable]]]
    val hadoopRDD = createHadoopRdd(tableDesc, inputPathStr, ifc)

    val deserializedHadoopRDD = hadoopRDD.mapPartitions { iter =>
      val hconf = broadcastedHiveConf.value.value
      val deserializer = tableDesc.getDeserializerClass().newInstance()
      deserializer.initialize(hconf, tableDesc.getProperties)

      // Deserialize each Writable to get the row value.
      iter.map { value =>
        value match {
          case v: Writable => deserializer.deserialize(v)
          case _ => throw new RuntimeException("Failed to match " + value.toString)
        }
      }
    }
    deserializedHadoopRDD
  }

  override def makeRDDForTablePartitions(partitions: Seq[Partition]): RDD[_] =
    makeRDDForTablePartitions(partitions, filterOpt = None)
  
  /**
   * Create a HadoopRDD for every partition key specified in the query. Note that for on-disk Hive
   * tables, a data directory is created for each partition corresponding to keys specified using
   * 'PARTITION BY'.
   */
  def makeRDDForTablePartitions(
      partitions: Seq[Partition],
      filterOpt: Option[PathFilter]): RDD[_] = {
    val hivePartitionRDDs = partitions.map { partition =>
      val partDesc = Utilities.getPartitionDesc(partition)
      val partPath = partition.getPartitionPath
      val inputPathStr = applyFilterIfNeeded(partPath, filterOpt)
      val ifc = partDesc.getInputFileFormatClass
        .asInstanceOf[java.lang.Class[InputFormat[Writable, Writable]]]
      // Get partition field info
      val partSpec = partDesc.getPartSpec()
      val partProps = partDesc.getProperties()
      val partDeserializer = partDesc.getDeserializerClass()

      val partColsDelimited = partProps.getProperty(META_TABLE_PARTITION_COLUMNS)
      // Partitioning columns are delimited by "/"
      val partCols = partColsDelimited.trim().split("/").toSeq
      // 'partValues[i]' contains the value for the partitioning column at 'partCols[i]'.
      val partValues = if (partSpec == null) {
        Array.fill(partCols.size)(new String)
      } else {
        partCols.map(col => new String(partSpec.get(col))).toArray
      }

      // Create local references so that the outer object isn't serialized.
      val tableDesc = _tableDesc
      val broadcastedHiveConf = _broadcastedHiveConf

      val hivePartitionRDD = createHadoopRdd(tableDesc, inputPathStr, ifc)
      hivePartitionRDD.mapPartitions { iter =>
        val hconf = broadcastedHiveConf.value.value
        val rowWithPartArr = new Array[Object](2)
        // Map each tuple to a row object
        iter.map { value =>
          val deserializer = partDeserializer.newInstance()
          deserializer.initialize(hconf, partProps)
          val deserializedRow = deserializer.deserialize(value) // LazyStruct
          rowWithPartArr.update(0, deserializedRow)
          rowWithPartArr.update(1, partValues)
          rowWithPartArr.asInstanceOf[Object]
        }
      }
    }
    // Even if we don't use any partitions, we still need an empty RDD
    if (hivePartitionRDDs.size == 0) {
      new EmptyRDD[Object](SharkEnv.sc)
    } else {
      new UnionRDD(hivePartitionRDDs(0).context, hivePartitionRDDs)
    }
  }

  private def applyFilterIfNeeded(path: Path, filterOpt: Option[PathFilter]): String = {
    filterOpt match {
      case Some(filter) => {
        val fs = path.getFileSystem(_localHConf)
        val filteredFiles = fs.listStatus(path, filter).map(_.getPath.toString)
        filteredFiles.mkString(",")
      }
      case None => path.toString
    }
  }

  /**
   * Creates a HadoopRDD based on the broadcasted HiveConf and other job properties that will be
   * applied locally on each slave.
   */
  private def createHadoopRdd(
      tableDesc: TableDesc,
      path: String,
      inputFormatClass: Class[InputFormat[Writable, Writable]])
    : RDD[Writable] = {
    val initializeJobConfFunc = HadoopTableReader.initializeLocalJobConfFunc(path, tableDesc) _

    val rdd = new HadoopRDD(
      SharkEnv.sc,
      _broadcastedHiveConf.asInstanceOf[Broadcast[SerializableWritable[Configuration]]],
      Some(initializeJobConfFunc),
      inputFormatClass,
      classOf[Writable],
      classOf[Writable],
      _minSplitsPerRDD)

    // Only take the value (skip the key) because Hive works only with values.
    rdd.map(_._2)
  }

}

object HadoopTableReader {

  /**
   * Curried. After given an argument for 'path', the resulting JobConf => Unit closure is used to
   * instantiate a HadoopRDD.
   */
  def initializeLocalJobConfFunc(path: String, tableDesc: TableDesc)(jobConf: JobConf) {
    FileInputFormat.setInputPaths(jobConf, path)
    if (tableDesc != null) {
      Utilities.copyTableJobPropertiesToConf(tableDesc, jobConf)
    }
    val bufferSize = System.getProperty("spark.buffer.size", "65536")
    jobConf.set("io.file.buffer.size", bufferSize)
  }

  /** Adds S3 credentials to the `conf`. */
  def addCredentialsToConf(conf: Configuration) {
    // Set s3/s3n credentials. Setting them in localJobConf ensures the settings propagate
    // from Spark's master all the way to Spark's slaves.
    var s3varsSet = false
    val s3vars = Seq("fs.s3n.awsAccessKeyId", "fs.s3n.awsSecretAccessKey",
      "fs.s3.awsAccessKeyId", "fs.s3.awsSecretAccessKey").foreach { variableName =>
      if (conf.get(variableName) != null) {
        s3varsSet = true
      }
    }

    // If none of the s3 credentials are set in Hive conf, try use the environmental
    // variables for credentials.
    if (!s3varsSet) {
      Utils.setAwsCredentials(conf)
    }
  }
}
