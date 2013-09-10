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

import scala.collection.JavaConversions._
import scala.collection.mutable.ConcurrentMap

import org.apache.spark.rdd.{RDD, UnionRDD}
import org.apache.spark.storage.StorageLevel

import shark.SharkConfVars


class MemoryMetadataManager {

  private val _keyToMemoryTable: ConcurrentMap[String, MemoryTable] =
    new ConcurrentHashMap[String, MemoryTable]()

  // TODO(harvey): Support stats for cached Hive-partitioned tables.
  private val _keyToStats: ConcurrentMap[String, collection.Map[Int, TablePartitionStats]] =
    new ConcurrentHashMap[String, collection.Map[Int, TablePartitionStats]]

  def contains(key: String) = _keyToMemoryTable.contains(key.toLowerCase)

  def add(key: String, isHivePartitioned: Boolean) {
    _keyToMemoryTable.put(key, new MemoryTable(key, isHivePartitioned))
  }

  def put(key: String, rdd: RDD[_]) {
    _keyToMemoryTable(key.toLowerCase).tableRDD = rdd
  }

  def putHivePartition(key: String, partitionColumn: String, rdd: RDD[_]) {
    _keyToMemoryTable(key.toLowerCase).hivePartitionRDDs(partitionColumn) = rdd
  }

  def get(key: String): Option[RDD[_]] = {
    val memoryTableValue: Option[MemoryTable] = _keyToMemoryTable.get(key.toLowerCase)
    return memoryTableValue.flatMap(_.tableRDD)
  }

  def getHivePartition(key: String, partitionColumn: String): Option[RDD[_]] = {
    return _keyToMemoryTable(key.toLowerCase).hivePartitionRDDs.get(partitionColumn)
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
   * @return Option::isEmpty() is true of there is no MemoryTable corresponding to 'key' in
   *         _keyToMemoryTable. For MemoryTables that are Hive-partitioned, the RDD returned will be
   *         a UnionRDD comprising all RDDs for all Hive-partitions.
   */
  def unpersist(key: String): Option[RDD[_]] = {
    def unpersistRDD(rdd: RDD[_]) {
      rdd match {
        case u: UnionRDD[_] => {
          // Recursively unpersist() all RDDs that compose the UnionRDD.
          u.unpersist()
          u.rdds.foreach {
            r => unpersistRDD(r)
          }
        }
        case r => r.unpersist()
      }
    }
    def unpersistMemoryTable(memoryTable: MemoryTable): Option[RDD[_]] = {
      if (memoryTable.isHivePartitioned) {
        // unpersist() all RDDs for all Hive-partitions.
        val hivePartitionRDDs =
          memoryTable.hivePartitionRDDs.values.toSeq.asInstanceOf[Seq[RDD[Any]]]
        if (hivePartitionRDDs.size > 0) {
          return Some(new UnionRDD(hivePartitionRDDs.head.context, hivePartitionRDDs))
        }
        return None
      } else {
        if (memoryTable.tableRDD.isDefined) {
          unpersistRDD(memoryTable.tableRDD.get)
        }
        return memoryTable.tableRDD
      }
    }

    // Remove MemoryTable's entry from Shark metadata.
    _keyToStats.remove(key)

    val memoryTableValue: Option[MemoryTable] = _keyToMemoryTable.remove(key.toLowerCase)
    return memoryTableValue.flatMap(unpersistMemoryTable(_))
  }
}


object MemoryMetadataManager {

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
