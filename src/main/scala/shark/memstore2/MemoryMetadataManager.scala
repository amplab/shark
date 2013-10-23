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
import java.util.{HashMap => JavaHashMap, Map => JavaMap}

import scala.collection.JavaConversions._
import scala.collection.mutable.ConcurrentMap

import org.apache.spark.rdd.{RDD, UnionRDD}
import org.apache.spark.storage.StorageLevel

import shark.execution.RDDUtils
import shark.SharkConfVars
import shark.SharkEnv


class MemoryMetadataManager {

  private val _keyToTable: ConcurrentMap[String, Table] =
    new ConcurrentHashMap[String, Table]()

  // TODO(harvey): Support stats for Hive-partitioned tables.
  private val _keyToStats: ConcurrentMap[String, collection.Map[Int, TablePartitionStats]] =
    new ConcurrentHashMap[String, collection.Map[Int, TablePartitionStats]]

  def putStats(key: String, stats: collection.Map[Int, TablePartitionStats]) {
    _keyToStats.put(key.toLowerCase, stats)
  }

  def getStats(key: String): Option[collection.Map[Int, TablePartitionStats]] = {
    _keyToStats.get(key.toLowerCase)
  }

  def createMemoryTable(
      tableName: String,
      cacheMode: CacheType.CacheType,
      preferredStorageLevel: StorageLevel
    ): MemoryTable = {
    var newTable = new MemoryTable(tableName.toLowerCase, cacheMode, preferredStorageLevel)
    _keyToTable.put(tableName.toLowerCase, newTable)
    return newTable
  }

  def createPartitionedMemoryTable(
      tableName: String,
      cacheMode: CacheType.CacheType,
      preferredStorageLevel: StorageLevel,
      tblProps: JavaMap[String, String]
    ): PartitionedMemoryTable = {
    var newTable = new PartitionedMemoryTable(
      tableName.toLowerCase, cacheMode, preferredStorageLevel)
    val shouldUseCachePolicy = tblProps.getOrElse(
      SharkConfVars.SHOULD_USE_CACHE_POLICY.varname,
      SharkConfVars.SHOULD_USE_CACHE_POLICY.defaultBoolVal.toString).toBoolean
    if (shouldUseCachePolicy) {
      // Determine the cache policy to use and read any user-specified cache settings.
      val cachePolicyStr = tblProps.getOrElse(
        SharkConfVars.CACHE_POLICY.varname,
        SharkConfVars.CACHE_POLICY.defaultVal)
      val maxCacheSize = tblProps.getOrElse(
        SharkConfVars.MAX_PARTITION_CACHE_SIZE.varname,
        SharkConfVars.MAX_PARTITION_CACHE_SIZE.defaultVal).toInt
      newTable.setPartitionCachePolicy(cachePolicyStr, maxCacheSize)
    }

    _keyToTable.put(tableName.toLowerCase, newTable)
    return newTable
  }

  def isHivePartitioned(tableName: String): Boolean = {
    _keyToTable.get(tableName.toLowerCase) match {
      case Some(table) => return table.isInstanceOf[PartitionedMemoryTable]
      case None => return false
    }
  }

  def containsTable(tableName: String): Boolean = _keyToTable.contains(tableName.toLowerCase)

  def getTable(tableName: String): Option[Table] = _keyToTable.get(tableName.toLowerCase)

  def getMemoryTable(tableName: String): Option[MemoryTable] = {
    val tableOpt = _keyToTable.get(tableName.toLowerCase)
    if (tableOpt.isDefined) {
     assert(tableOpt.get.isInstanceOf[MemoryTable],
       "getMemoryTable() called for a partitioned table.")
    }
    tableOpt.asInstanceOf[Option[MemoryTable]]
  }

  def getPartitionedTable(tableName: String): Option[PartitionedMemoryTable] = {
    val tableOpt = _keyToTable.get(tableName.toLowerCase)
    if (tableOpt.isDefined) {
      assert(tableOpt.get.isInstanceOf[PartitionedMemoryTable],
        "getPartitionedTable() called for a non-partitioned table.")
    }
    tableOpt.asInstanceOf[Option[PartitionedMemoryTable]]
  }

  def renameTable(oldName: String, newName: String) {
    val lowerCaseOldName = oldName.toLowerCase
    if (containsTable(lowerCaseOldName)) {
      val lowerCaseNewName = newName.toLowerCase

      val statsValueEntry = _keyToStats.remove(lowerCaseOldName).get
      val tableValueEntry = _keyToTable.remove(lowerCaseOldName).get
      tableValueEntry.tableName = lowerCaseNewName

      _keyToStats.put(lowerCaseNewName, statsValueEntry)
      _keyToTable.put(lowerCaseNewName, tableValueEntry)
    }
  }

  /**
   * Used to drop a table from the Spark in-memory cache and/or disk. All metadata tracked by Shark
   * (e.g. entry in '_keyToStats' if the table isn't Hive-partitioned) is deleted as well.
   *
   * @param key Name of the table to drop.
   * @return Option::isEmpty() is true of there is no MemoryTable (and RDD) corresponding to 'key'
   *     in _keyToMemoryTable. For MemoryTables that are Hive-partitioned, the RDD returned will
   *     be a UnionRDD comprising RDDs that represent the table's Hive-partitions.
   */
  def removeTable(tableName: String): Option[RDD[_]] = {
    val lowerCaseTableName = tableName.toLowerCase

    // Remove MemoryTable's entry from Shark metadata.
    _keyToStats.remove(lowerCaseTableName)

    val tableValue: Option[Table] = _keyToTable.remove(lowerCaseTableName)
    return tableValue.flatMap(MemoryMetadataManager.unpersistRDDsInTable(_))
  }

  /** Find all keys that are strings. Used to drop tables after exiting. */
  def getAllKeyStrings(): Seq[String] = {
    _keyToTable.keys.collect { case k: String => k } toSeq
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
    return unpersistedRDD
  }

  /**
   * Return a representation of the partition key in the string format:
   *     'col1=value1/col2=value2/...'
   */
  def makeHivePartitionKeyStr(
      partitionColumns: Seq[String],
      partitionColumnToValue: JavaMap[String, String]): String = {
    var keyStr = ""
    for (partitionColumn <- partitionColumns) {
      keyStr += "%s=%s/".format(partitionColumn, partitionColumnToValue(partitionColumn))
    }
    keyStr = keyStr.dropRight(1)
    return keyStr
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
