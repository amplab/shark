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

import shark.SharkConfVars
import shark.SharkEnv


// TODO(harvey): Redo the interfaces to this class. For example, add() could be renamed to
//               addCreatedTable(), which should also take in a Hive DB (metastore) name.
class MemoryMetadataManager {

  private val _keyToMemoryTable: ConcurrentMap[String, MemoryTable] =
    new ConcurrentHashMap[String, MemoryTable]()

  // TODO(harvey): Support stats for cached Hive-partitioned tables.
  private val _keyToStats: ConcurrentMap[String, collection.Map[Int, TablePartitionStats]] =
    new ConcurrentHashMap[String, collection.Map[Int, TablePartitionStats]]

  def add(key: String, isHivePartitioned: Boolean, cacheMode: CacheType.CacheType) {
    val memoryTable = new MemoryTable(key.toLowerCase, isHivePartitioned)
    if (isHivePartitioned) {
      memoryTable.keyToHivePartitions = new JavaHashMap[String, RDD[_]]()
    }
    memoryTable.cacheMode = cacheMode
    _keyToMemoryTable(key.toLowerCase) = memoryTable
  }

  def getCacheMode(key: String): CacheType.CacheType = {
    _keyToMemoryTable.get(key.toLowerCase) match {
      case Some(memoryTable) => return memoryTable.cacheMode
      case _ => return CacheType.NONE
    }
  }

  def isHivePartitioned(key: String): Boolean = {
    _keyToMemoryTable.get(key.toLowerCase) match {
      case Some(memoryTable) => return memoryTable.isHivePartitioned
      case None => return false
    }
  }

  def contains(key: String): Boolean = _keyToMemoryTable.contains(key.toLowerCase)

  def containsHivePartition(key: String, partitionColumnValues: String): Boolean = {
    val containsTable = _keyToMemoryTable.contains(key.toLowerCase)
    return (containsTable &&
            _keyToMemoryTable(key.toLowerCase).keyToHivePartitions.contains(partitionColumnValues))
  }

  def put(key: String, rdd: RDD[_]) {
    val memoryTable = _keyToMemoryTable(key.toLowerCase)
    memoryTable.tableRDD = rdd
  }

  def putHivePartition(
      key: String,
      partitionColumnValues: String,
      rdd: RDD[_]) {
    val keyToHivePartitions = _keyToMemoryTable(key.toLowerCase).keyToHivePartitions
    keyToHivePartitions(partitionColumnValues) = rdd
  }

  def dropHivePartition(key: String, partitionColumnValues: String) {
    val keyToHivePartitions = _keyToMemoryTable(key.toLowerCase).keyToHivePartitions
    val rdd = keyToHivePartitions.remove(partitionColumnValues)
    unpersistRDD(rdd.get)
  }

  def get(key: String): Option[RDD[_]] = {
    _keyToMemoryTable.get(key.toLowerCase) match {
      case Some(memoryTable) => return Some(memoryTable.tableRDD)
      case _ => return None
    }
  }

  def getHivePartition(key: String, partitionColumnValues: String): Option[RDD[_]] = {
    val keyToHivePartitions = _keyToMemoryTable(key.toLowerCase).keyToHivePartitions
    keyToHivePartitions.get(partitionColumnValues)
  }

  def putStats(key: String, stats: collection.Map[Int, TablePartitionStats]) {
    _keyToStats.put(key.toLowerCase, stats)
  }

  def getStats(key: String): Option[collection.Map[Int, TablePartitionStats]] = {
    _keyToStats.get(key.toLowerCase)
  }

  /**
   * Find all keys that are strings. Used to drop tables after exiting.
   */
  def getAllKeyStrings(): Seq[String] = {
    _keyToMemoryTable.keys.collect { case k: String => k } toSeq
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
  def unpersist(key: String): Option[RDD[_]] = {
    def unpersistMemoryTable(memoryTable: MemoryTable): Option[RDD[_]] = {
      var unpersistedRDD: Option[RDD[_]] = None
      if (memoryTable.isHivePartitioned) {
        // unpersist() all RDDs for all Hive-partitions.
        val unpersistedRDDs =  memoryTable.keyToHivePartitions.values.map(
          rdd => unpersistRDD(rdd)).asInstanceOf[Seq[RDD[Any]]]
        if (unpersistedRDDs.size > 0) {
          val unionedRDD = new UnionRDD(unpersistedRDDs.head.context, unpersistedRDDs)
          unpersistedRDD = Some(unionedRDD)
        }
      } else {
        unpersistedRDD = Some(unpersistRDD(memoryTable.tableRDD))
      }
      return unpersistedRDD
    }

    // Remove MemoryTable's entry from Shark metadata.
    _keyToStats.remove(key.toLowerCase)

    val memoryTableValue: Option[MemoryTable] = _keyToMemoryTable.remove(key.toLowerCase)
    return memoryTableValue.flatMap(unpersistMemoryTable(_))
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
