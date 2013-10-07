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

  // TODO(harvey): Support stats for cached Hive-partitioned tables.
  private val _keyToStats: ConcurrentMap[String, collection.Map[Int, TablePartitionStats]] =
    new ConcurrentHashMap[String, collection.Map[Int, TablePartitionStats]]

  def createMemoryTable(
      tableName: String,
      cacheMode: CacheType.CacheType
    ): MemoryTable = {
    var newTable = new MemoryTable(tableName.toLowerCase, cacheMode)
    _keyToTable.put(tableName.toLowerCase, newTable)
    return newTable
  }

  def createPartitionedMemoryTable(
      tableName: String,
      cacheMode: CacheType.CacheType,
      cachePolicyStr: String,
      cachePolicyMaxSize: Long,
      shouldRecordStats: Boolean
    ): PartitionedMemoryTable = {
    var newTable = new PartitionedMemoryTable(tableName.toLowerCase, cacheMode)
    newTable.setPartitionCachePolicy(cachePolicyStr, cachePolicyMaxSize, shouldRecordStats)
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
   val tableFound = _keyToTable.get(tableName.toLowerCase)
   tableFound.foreach(table =>
     assert(table.isInstanceOf[MemoryTable],
       "getMemoryTable() called for a partitioned table."))

   tableFound.asInstanceOf[Option[MemoryTable]]
  }

  def getPartitionedTable(tableName: String): Option[PartitionedMemoryTable] = {
   val tableFound = _keyToTable.get(tableName.toLowerCase)
   tableFound.foreach(table =>
     assert(table.isInstanceOf[PartitionedMemoryTable],
       "getPartitionedTable() called for a non-partitioned table."))

   tableFound.asInstanceOf[Option[PartitionedMemoryTable]]
  }

  def putStats(key: String, stats: collection.Map[Int, TablePartitionStats]) {
    _keyToStats.put(key.toLowerCase, stats)
  }

  def getStats(key: String): Option[collection.Map[Int, TablePartitionStats]] = {
    _keyToStats.get(key.toLowerCase)
  }

  def renameTable(oldName: String, newName: String) {
    if (containsTable(oldName)) {
      val lowerCaseOldName = oldName.toLowerCase
      val lowerCaseNewName = newName.toLowerCase

      val statsValueEntry = _keyToStats.remove(lowerCaseOldName).get
      val tableValueEntry = _keyToTable.remove(lowerCaseOldName).get
      tableValueEntry.tableName = lowerCaseNewName

      _keyToStats.put(lowerCaseNewName, statsValueEntry)
      _keyToTable.put(lowerCaseNewName, tableValueEntry)
    }
  }

  /**
   * Find all keys that are strings. Used to drop tables after exiting.
   */
  def getAllKeyStrings(): Seq[String] = {
    _keyToTable.keys.collect { case k: String => k } toSeq
  }

  /**
   * Used to drop a table from the Spark in-memory cache and/or disk. All metadata
   * (e.g. entry in '_keyToStats' if the table isn't Hive-partitioned) tracked by Shark is deleted
   * as well.
   *
   * @param key Name of the table to drop.
   * @return Option::isEmpty() is true of there is no MemoryTable (and RDD) corresponding to 'key'
   *         in _keyToMemoryTable. For MemoryTables that are Hive-partitioned, the RDD returned will
   *         be a UnionRDD comprising RDDs that represent the table's Hive-partitions.
   */
  def unpersist(tableName: String): Option[RDD[_]] = {
    val lowerCaseTableName = tableName.toLowerCase

    def unpersistTable(table: Table): Option[RDD[_]] = {
      var unpersistedRDD: Option[RDD[_]] = None
      if (table.isInstanceOf[PartitionedMemoryTable]) {
        val partitionedTable = table.asInstanceOf[PartitionedMemoryTable]
        // unpersist() all RDDs for all Hive-partitions.
        val unpersistedRDDs =  partitionedTable.getAllPartitions.map(
          rdd => unpersistRDD(rdd)).asInstanceOf[Seq[RDD[Any]]]
        if (unpersistedRDDs.size > 0) {
          val unionedRDD = new UnionRDD(unpersistedRDDs.head.context, unpersistedRDDs)
          unpersistedRDD = Some(unionedRDD)
        }
      } else {
        unpersistedRDD = Some(unpersistRDD(table.asInstanceOf[MemoryTable].tableRDD))
      }
      return unpersistedRDD
    }

    // Remove MemoryTable's entry from Shark metadata.
    _keyToStats.remove(lowerCaseTableName)

    val tableValue: Option[Table] = _keyToTable.remove(lowerCaseTableName)
    return tableValue.flatMap(unpersistTable(_))
  }

  def unpersistRDD(rdd: RDD[_]): RDD[_] = {
    rdd match {
      case u: UnionRDD[_] => {
        // Recursively unpersist() all RDDs that compose the UnionRDD.
        u.unpersist()
        u.rdds.map {
          r => r.unpersist()
        }
      }
      case r => r.unpersist()
    }
    return rdd
  }
}


object MemoryMetadataManager {

  def makeHivePartitionKeyStr(
      partitionColumns: Seq[String],
      partitionColumnToValue: JavaMap[String, String]): String = {
    // The keyStr is the string 'col1=value1/col2=value2'.
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
