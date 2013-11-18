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

import org.apache.hadoop.hive.metastore.api.Constants.META_TABLE_PARTITION_COLUMNS
import org.apache.hadoop.hive.ql.exec.Utilities
import org.apache.hadoop.hive.ql.metadata.{Partition => HivePartition, Table => HiveTable}
import org.apache.hadoop.hive.ql.plan.TableDesc

import org.apache.spark.rdd.{EmptyRDD, RDD, UnionRDD}

import shark.{LogHelper, SharkEnv}
import shark.api.QueryExecutionException
import shark.execution.serialization.JavaSerializer
import shark.memstore2.{MemoryMetadataManager, TablePartition, TablePartitionStats}
import shark.tachyon.TachyonException


/**
 * A trait for subclasses that handle table scans. In Shark, there is one subclass for each
 * type of table storage: HeapTableReader for Shark tables in Spark's block manager,
 * TachyonTableReader for tables in Tachyon, and HadoopTableReader for Hive tables in a filesystem.
 */
trait TableReader extends LogHelper{

  def makeRDDForTable(hiveTable: HiveTable): RDD[_]

  def makeRDDForPartitionedTable(partitions: Seq[HivePartition]): RDD[_]

}

/** Helper class for scanning tables stored in Tachyon. */
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

    // True if stats for the target table is missing from the Shark metastore, and should be fetched
    // and deserialized from Tachyon's metastore. This can happen if that table was created in a
    // previous Shark session, since Shark's metastore is not persistent.
    val shouldFetchStatsFromTachyon = SharkEnv.memoryMetadataManager.getStats(
      _databaseName, _tableName).isEmpty
    if (shouldFetchStatsFromTachyon) {
      val statsByteBuffer = SharkEnv.tachyonUtil.getTableMetadata(tableKey)
      val indexToStats = JavaSerializer.deserialize[collection.Map[Int, TablePartitionStats]](
        statsByteBuffer.array())
      logInfo("Loading table " + tableKey + " stats from Tachyon.")
      SharkEnv.memoryMetadataManager.putStats(_databaseName, _tableName, indexToStats)
    }
    SharkEnv.tachyonUtil.createRDD(tableKey)
  }

  override def makeRDDForPartitionedTable(partitions: Seq[HivePartition]): RDD[_] = {
    throw new UnsupportedOperationException("Partitioned tables are not yet supported for Tachyon.")
  }
}

/** Helper class for scanning tables stored in Spark's block manager */
class HeapTableReader(@transient _tableDesc: TableDesc) extends TableReader {

  // Split from 'databaseName.tableName'
  private val _tableNameSplit = _tableDesc.getTableName.split('.')
  private val _databaseName = _tableNameSplit(0)
  private val _tableName = _tableNameSplit(1)

  /** Fetches the RDD for `_tableName` from the Shark metastore. */
  override def makeRDDForTable(hiveTable: HiveTable): RDD[_] = {
    logInfo("Loading table %s.%s from Spark block manager".format(_databaseName, _tableName))
    val tableOpt = SharkEnv.memoryMetadataManager.getMemoryTable(_databaseName, _tableName)
    if (tableOpt.isEmpty) {
      throwMissingTableException()
    }

    val table = tableOpt.get
    table.tableRDD
  }

  /**
   * Fetch an RDD from the Shark metastore using each partition key given, and return a union of all
   * the fetched RDDs.
   *
   * @param partitions A collection of Hive-partition metadata, such as partition columns and
   *     partition key specifications.
   */
  override def makeRDDForPartitionedTable(partitions: Seq[HivePartition]): RDD[_] = {
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
      if (hivePartitionedTableOpt.isEmpty) {
        throwMissingTableException()
      }
      val hivePartitionedTable = hivePartitionedTableOpt.get

      val hivePartitionRDDOpt = hivePartitionedTable.getPartition(partitionKeyStr)
      if (hivePartitionRDDOpt.isEmpty) throwMissingPartitionException(partitionKeyStr)
      val hivePartitionRDD = hivePartitionRDDOpt.get

      hivePartitionRDD.mapPartitions { iter =>
        if (iter.hasNext) {
          // Map each tuple to a row object
          val rowWithPartArr = new Array[Object](2)
          val tablePartition: TablePartition = iter.next()
          tablePartition.iterator.map { value =>
            rowWithPartArr.update(0, value.asInstanceOf[Object])
            rowWithPartArr.update(1, partValues)
            rowWithPartArr.asInstanceOf[Object]
          }
        } else {
         Iterator.empty
        }
      }
    }
    if (hivePartitionRDDs.size > 0) {
      new UnionRDD(hivePartitionRDDs.head.context, hivePartitionRDDs)
    } else {
      new EmptyRDD[Object](SharkEnv.sc)
    }
  }

  /**
   * Thrown if the table identified by the (_databaseName, _tableName) pair cannot be found in
   * the Shark metastore.
   */
  private def throwMissingTableException() {
    logError("""|Table %s.%s not found in block manager.
                |Are you trying to access a cached table from a Shark session other than the one
                |in which it was created?""".stripMargin.format(_databaseName, _tableName))
    throw new QueryExecutionException("Cached table not found")
  }

  /**
   * Thrown if the table partition identified by the (_databaseName, _tableName, partValues) tuple
   * cannot be found in the Shark metastore.
   */
  private def throwMissingPartitionException(partValues: String) {
    logError("""|Partition %s for table %s.%s not found in block manager.
                |Are you trying to access a cached table from a Shark session other than the one in
                |which it was created?""".stripMargin.format(partValues, _databaseName, _tableName))
    throw new QueryExecutionException("Cached table partition not found")
  }
}
