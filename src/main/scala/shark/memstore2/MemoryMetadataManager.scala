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
import java.util.{Map => JavaMap}

import scala.collection.JavaConversions._
import scala.collection.mutable.ConcurrentMap

import org.apache.spark.rdd.{RDD, UnionRDD}
import org.apache.spark.storage.StorageLevel

import shark.execution.RDDUtils
import shark.SharkConfVars


class MemoryMetadataManager {

  private val _keyToTable: ConcurrentMap[String, Table] =
    new ConcurrentHashMap[String, Table]()

  // TODO(harvey): Support stats for Hive-partitioned tables.
  private val _keyToStats: ConcurrentMap[String, collection.Map[Int, TablePartitionStats]] =
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
    _keyToTable.get(tableKey) match {
      case Some(table) => table.isInstanceOf[PartitionedMemoryTable]
      case None => false
    }
  }

  def containsTable(databaseName: String, tableName: String): Boolean = {
    _keyToTable.contains(makeTableKey(databaseName, tableName))
  }

  def createMemoryTable(
      databaseName: String,
      tableName: String,
      cacheMode: CacheType.CacheType,
      preferredStorageLevel: StorageLevel
    ): MemoryTable = {
    val tableKey = makeTableKey(databaseName, tableName)
    val newTable = new MemoryTable(tableKey, cacheMode, preferredStorageLevel)
    _keyToTable.put(tableKey, newTable)
    newTable
  }

  def createPartitionedMemoryTable(
      databaseName: String,
      tableName: String,
      cacheMode: CacheType.CacheType,
      preferredStorageLevel: StorageLevel,
      tblProps: JavaMap[String, String]
    ): PartitionedMemoryTable = {
    val tableKey = makeTableKey(databaseName, tableName)
    val newTable = new PartitionedMemoryTable(tableKey, cacheMode, preferredStorageLevel)
    // Determine the cache policy to use and read any user-specified cache settings.
    val cachePolicyStr = tblProps.getOrElse(SharkConfVars.CACHE_POLICY.varname,
      SharkConfVars.CACHE_POLICY.defaultVal)
    val maxCacheSize = tblProps.getOrElse(SharkConfVars.MAX_PARTITION_CACHE_SIZE.varname,
      SharkConfVars.MAX_PARTITION_CACHE_SIZE.defaultVal).toInt
    newTable.setPartitionCachePolicy(cachePolicyStr, maxCacheSize)

    _keyToTable.put(tableKey, newTable)
    newTable
  }

  def getTable(databaseName: String, tableName: String): Option[Table] = {
    _keyToTable.get(makeTableKey(databaseName, tableName))
  }

  def getMemoryTable(databaseName: String, tableName: String): Option[MemoryTable] = {
    val tableKey = makeTableKey(databaseName, tableName)
    val tableOpt = _keyToTable.get(tableKey)
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
    val tableOpt = _keyToTable.get(tableKey)
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
      val tableValueEntry = _keyToTable.remove(oldTableKey).get
      tableValueEntry.tableName = newTableKey

      _keyToStats.put(newTableKey, statsValueEntry)
      _keyToTable.put(newTableKey, tableValueEntry)
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
    val tableValue: Option[Table] = _keyToTable.remove(tableKey)
    tableValue.flatMap(MemoryMetadataManager.unpersistRDDsInTable(_))
  }

  /**
   * Find all keys that are strings. Used to drop tables after exiting.
   *
   * TODO(harvey): Won't be needed after unifed views are added.
   */
  def getAllKeyStrings(): Seq[String] = {
    _keyToTable.keys.collect { case k: String => k } toSeq
  }

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

  /** Return a StorageLevel corresponding to its String name. */
  def getStorageLevelFromString(s: String): StorageLevel = {
    if (s == null || s == "") {
      getStorageLevelFromString(SharkConfVars.STORAGE_LEVEL.defaultVal)
    } else {
      s.toUpperCase match {
        case "NONE" => StorageLevel.NONE
        case "DISK_ONLY" => StorageLevel.DISK_ONLY
        case "DISK_ONLY_2" => StorageLevel.DISK_ONLY_2
        case "MEMORY_ONLY" => StorageLevel.MEMORY_ONLY
        case "MEMORY_ONLY_2" => StorageLevel.MEMORY_ONLY_2
        case "MEMORY_ONLY_SER" => StorageLevel.MEMORY_ONLY_SER
        case "MEMORY_ONLY_SER_2" => StorageLevel.MEMORY_ONLY_SER_2
        case "MEMORY_AND_DISK" => StorageLevel.MEMORY_AND_DISK
        case "MEMORY_AND_DISK_2" => StorageLevel.MEMORY_AND_DISK_2
        case "MEMORY_AND_DISK_SER" => StorageLevel.MEMORY_AND_DISK_SER
        case "MEMORY_AND_DISK_SER_2" => StorageLevel.MEMORY_AND_DISK_SER_2
        case _ => throw new IllegalArgumentException("Unrecognized storage level: " + s)
      }
    }
  }
}
