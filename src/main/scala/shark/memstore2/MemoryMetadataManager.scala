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

import java.util.concurrent.ConcurrentHashMap
import java.util.{HashMap=> JavaHashMap, Map => JavaMap}

import scala.collection.JavaConversions._
import scala.collection.concurrent

import org.apache.hadoop.hive.ql.metadata.Hive

import org.apache.spark.rdd.{RDD, UnionRDD}

import shark.execution.RDDUtils
import shark.util.HiveUtils


class MemoryMetadataManager {

  // Set of tables, from databaseName.tableName to Table object.
  private val _tables: concurrent.Map[String, Table] =
    new ConcurrentHashMap[String, Table]()

  // TODO(harvey): Support stats for Hive-partitioned tables.
  // Set of stats, from databaseName.tableName to the stats. This is guaranteed to have the same
  // structure / size as the _tables map.
  private val _keyToStats: concurrent.Map[String, collection.Map[Int, TablePartitionStats]] =
    new ConcurrentHashMap[String, collection.Map[Int, TablePartitionStats]]

  def putStats(
      databaseName: String,
      tableName: String,
      stats: collection.Map[Int, TablePartitionStats]) {
    val tableKey = makeTableKey(databaseName, tableName)
    _keyToStats.put(tableKey, stats)
  }

  def getStats(
      databaseName: String,
      tableName: String): Option[collection.Map[Int, TablePartitionStats]] = {
    val tableKey = makeTableKey(databaseName, tableName)
    _keyToStats.get(tableKey)
  }

  def isHivePartitioned(databaseName: String, tableName: String): Boolean = {
    val tableKey = makeTableKey(databaseName, tableName)
    _tables.get(tableKey) match {
      case Some(table) => table.isInstanceOf[PartitionedMemoryTable]
      case None => false
    }
  }

  def containsTable(databaseName: String, tableName: String): Boolean = {
    _tables.contains(makeTableKey(databaseName, tableName))
  }

  def createMemoryTable(
      databaseName: String,
      tableName: String,
      cacheMode: CacheType.CacheType): MemoryTable = {
    val tableKey = makeTableKey(databaseName, tableName)
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
    val tableKey = makeTableKey(databaseName, tableName)
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
    _tables.get(makeTableKey(databaseName, tableName))
  }

  def getMemoryTable(databaseName: String, tableName: String): Option[MemoryTable] = {
    val tableKey = makeTableKey(databaseName, tableName)
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
    val tableKey = makeTableKey(databaseName, tableName)
    val tableOpt = _tables.get(tableKey)
    if (tableOpt.isDefined) {
      assert(tableOpt.get.isInstanceOf[PartitionedMemoryTable],
        "getPartitionedTable() called for a non-partitioned table.")
    }
    tableOpt.asInstanceOf[Option[PartitionedMemoryTable]]
  }

  def renameTable(databaseName: String, oldName: String, newName: String) {
    if (containsTable(databaseName, oldName)) {
      val oldTableKey = makeTableKey(databaseName, oldName)
      val newTableKey = makeTableKey(databaseName, newName)

      val statsValueEntry = _keyToStats.remove(oldTableKey).get
      val tableValueEntry = _tables.remove(oldTableKey).get
      tableValueEntry.tableName = newTableKey

      _keyToStats.put(newTableKey, statsValueEntry)
      _tables.put(newTableKey, tableValueEntry)
    }
  }

  /**
   * Used to drop a table from the Spark in-memory cache and/or disk. All metadata tracked by Shark
   * (e.g. entry in '_keyToStats' if the table isn't Hive-partitioned) is deleted as well.
   *
   * @return Option::isEmpty() is true of there is no MemoryTable (and RDD) corresponding to 'key'
   *     in _keyToMemoryTable. For MemoryTables that are Hive-partitioned, the RDD returned will
   *     be a UnionRDD comprising RDDs that represent the table's Hive-partitions.
   */
  def removeTable(databaseName: String, tableName: String): Option[RDD[_]] = {
    val tableKey = makeTableKey(databaseName, tableName)

    // Remove MemoryTable's entry from Shark metadata.
    _keyToStats.remove(tableKey)
    val tableValue: Option[Table] = _tables.remove(tableKey)
    tableValue.flatMap(MemoryMetadataManager.unpersistRDDsInTable(_))
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
          // No need to handle Hive or Tachyon tables, which are persistent and managed by their
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

  // Returns the key "databaseName.tableName".
  private def makeTableKey(databaseName: String, tableName: String): String = {
    (databaseName + '.' + tableName).toLowerCase
  }
}


object MemoryMetadataManager {

  def unpersistRDDsInTable(table: Table): Option[RDD[_]] = {
    var unpersistedRDD: Option[RDD[_]] = None
    if (table.isInstanceOf[PartitionedMemoryTable]) {
      val partitionedTable = table.asInstanceOf[PartitionedMemoryTable]
      // unpersist() all RDDs for all Hive-partitions.
      val unpersistedRDDs =  partitionedTable.keyToPartitions.values.map(
        rdd => RDDUtils.unpersistRDD(rdd)).asInstanceOf[Seq[RDD[Any]]]
      if (unpersistedRDDs.size > 0) {
        val unionedRDD = new UnionRDD(unpersistedRDDs.head.context, unpersistedRDDs)
        unpersistedRDD = Some(unionedRDD)
      }
    } else {
      unpersistedRDD = Some(RDDUtils.unpersistRDD(table.asInstanceOf[MemoryTable].tableRDD))
    }
    unpersistedRDD
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
