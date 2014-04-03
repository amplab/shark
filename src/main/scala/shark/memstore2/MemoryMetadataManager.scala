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

package shark.memstore2

import java.util.{HashMap=> JavaHashMap, Map => JavaMap}
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConversions._
import scala.collection.concurrent

import org.apache.hadoop.hive.ql.metadata.Hive

import org.apache.spark.rdd.{RDD, UnionRDD}

import shark.{LogHelper, SharkEnv}
import shark.execution.RDDUtils
import shark.util.HiveUtils


class MemoryMetadataManager extends LogHelper {

  // Set of tables, from databaseName.tableName to Table object.
  private val _tables: concurrent.Map[String, Table] =
    new ConcurrentHashMap[String, Table]()

  def isHivePartitioned(databaseName: String, tableName: String): Boolean = {
    val tableKey = MemoryMetadataManager.makeTableKey(databaseName, tableName)
    _tables.get(tableKey) match {
      case Some(table) => table.isInstanceOf[PartitionedMemoryTable]
      case None => false
    }
  }

  def containsTable(databaseName: String, tableName: String): Boolean = {
    _tables.contains(MemoryMetadataManager.makeTableKey(databaseName, tableName))
  }

  def createMemoryTable(
      databaseName: String,
      tableName: String,
      cacheMode: CacheType.CacheType): MemoryTable = {
    val tableKey = MemoryMetadataManager.makeTableKey(databaseName, tableName)
    val newTable = new MemoryTable(databaseName, tableName, cacheMode)
    _tables.put(tableKey, newTable)
    newTable
  }

  def createPartitionedMemoryTable(
      databaseName: String,
      tableName: String,
      cacheMode: CacheType.CacheType,
      tblProps: JavaMap[String, String]
    ): PartitionedMemoryTable = {
    val tableKey = MemoryMetadataManager.makeTableKey(databaseName, tableName)
    val newTable = new PartitionedMemoryTable(databaseName, tableName, cacheMode)
    // Determine the cache policy to use and read any user-specified cache settings.
    val cachePolicyStr = tblProps.getOrElse(SharkTblProperties.CACHE_POLICY.varname,
      SharkTblProperties.CACHE_POLICY.defaultVal)
    val maxCacheSize = tblProps.getOrElse(SharkTblProperties.MAX_PARTITION_CACHE_SIZE.varname,
      SharkTblProperties.MAX_PARTITION_CACHE_SIZE.defaultVal).toInt
    newTable.setPartitionCachePolicy(cachePolicyStr, maxCacheSize)

    _tables.put(tableKey, newTable)
    newTable
  }

  def getTable(databaseName: String, tableName: String): Option[Table] = {
    _tables.get(MemoryMetadataManager.makeTableKey(databaseName, tableName))
  }

  def getMemoryTable(databaseName: String, tableName: String): Option[MemoryTable] = {
    val tableKey = MemoryMetadataManager.makeTableKey(databaseName, tableName)
    val tableOpt = _tables.get(tableKey)
    if (tableOpt.isDefined) {
     assert(tableOpt.get.isInstanceOf[MemoryTable],
       "getMemoryTable() called for a partitioned table.")
    }
    tableOpt.asInstanceOf[Option[MemoryTable]]
  }

  def getPartitionedTable(
      databaseName: String,
      tableName: String): Option[PartitionedMemoryTable] = {
    val tableKey = MemoryMetadataManager.makeTableKey(databaseName, tableName)
    val tableOpt = _tables.get(tableKey)
    if (tableOpt.isDefined) {
      assert(tableOpt.get.isInstanceOf[PartitionedMemoryTable],
        "getPartitionedTable() called for a non-partitioned table.")
    }
    tableOpt.asInstanceOf[Option[PartitionedMemoryTable]]
  }

  def renameTable(databaseName: String, oldName: String, newName: String) {
    if (containsTable(databaseName, oldName)) {
      val oldTableKey = MemoryMetadataManager.makeTableKey(databaseName, oldName)
      val newTableKey = MemoryMetadataManager.makeTableKey(databaseName, newName)

      val tableValueEntry = _tables.remove(oldTableKey).get
      tableValueEntry.tableName = newTableKey

      _tables.put(newTableKey, tableValueEntry)
    }
  }

  /**
   * Used to drop a table from Spark in-memory cache and/or disk. All metadata is deleted as well.
   *
   * Note that this is always used in conjunction with a dropTableFromMemory() for handling
   *'shark.cache' property changes in an ALTER TABLE command, or to finish off a DROP TABLE command
   * after the table has been deleted from the Hive metastore.
   *
   * @return Option::isEmpty() is true of there is no MemoryTable (and RDD) corresponding to 'key'
   *     in _keyToMemoryTable. For tables that are Hive-partitioned, the RDD returned will be a
   *     UnionRDD comprising RDDs that back the table's Hive-partitions.
   */
  def removeTable(
      databaseName: String,
      tableName: String): Option[RDD[_]] = {
    val tableKey = MemoryMetadataManager.makeTableKey(databaseName, tableName)
    val tableValueOpt: Option[Table] = _tables.remove(tableKey)
    tableValueOpt.flatMap(tableValue => MemoryMetadataManager.unpersistRDDsForTable(tableValue))
  }

  def shutdown() {
    val db = Hive.get()
    for (table <- _tables.values) {
      table.cacheMode match {
        case CacheType.MEMORY => {
          dropTableFromMemory(db, table.databaseName, table.tableName)
        }
        case CacheType.MEMORY_ONLY => HiveUtils.dropTableInHive(table.tableName, db.getConf)
        case _ => {
          // No need to handle Hive or off-heap tables, which are persistent and managed by their
          // respective systems.
          Unit
        }
      }
    }
  }

  /**
   * Drops a table from the Shark cache. However, Shark properties needed for table recovery
   * (see TableRecovery#reloadRdds()) won't be removed.
   * After this method completes, the table can still be scanned from disk.
   */
  def dropTableFromMemory(
      db: Hive,
      databaseName: String,
      tableName: String) {
    getTable(databaseName, tableName).foreach { sharkTable =>
      db.setCurrentDatabase(databaseName)
      val hiveTable = db.getTable(databaseName, tableName)
      // Refresh the Hive `db`.
      db.alterTable(tableName, hiveTable)
      // Unpersist the table's RDDs from memory.
      removeTable(databaseName, tableName)
    }
  }
}


object MemoryMetadataManager {

  def unpersistRDDsForTable(table: Table): Option[RDD[_]] = {
    table match {
      case partitionedTable: PartitionedMemoryTable => {
        // unpersist() all RDDs for all Hive-partitions.
        val unpersistedRDDs =  partitionedTable.keyToPartitions.values.map(rdd =>
          RDDUtils.unpersistRDD(rdd)).asInstanceOf[Seq[RDD[Any]]]
        if (unpersistedRDDs.size > 0) {
          val unionedRDD = new UnionRDD(unpersistedRDDs.head.context, unpersistedRDDs)
          Some(unionedRDD)
        } else {
          None
        }
      }
      case memoryTable: MemoryTable => Some(RDDUtils.unpersistRDD(memoryTable.getRDD.get))
    }
  }

  // Returns a key of the form "databaseName.tableName" that uniquely identifies a Shark table.
  // For example, it's used to track a table's RDDs in MemoryMetadataManager and table paths in the
  // off-heap table warehouse.
  def makeTableKey(databaseName: String, tableName: String): String = {
    (databaseName + '.' + tableName).toLowerCase
  }

  /**
   * Return a representation of the partition key in the string format:
   *     'col1=value1/col2=value2/.../colN=valueN'
   */
  def makeHivePartitionKeyStr(
      partitionCols: Seq[String],
      partColToValue: JavaMap[String, String]): String = {
    partitionCols.map(col => "%s=%s".format(col, partColToValue(col))).mkString("/")
  }

  /**
   * Returns a (partition column name -> value) mapping by parsing a `keyStr` of the format
   * 'col1=value1/col2=value2/.../colN=valueN', created by makeHivePartitionKeyStr() above.
   */
  def parseHivePartitionKeyStr(keyStr: String): JavaMap[String, String] = {
    val partitionSpec = new JavaHashMap[String, String]()
    for (pair <- keyStr.split("/")) {
      val pairSplit = pair.split("=")
      partitionSpec.put(pairSplit(0), pairSplit(1))
    }
    partitionSpec
  }
}
