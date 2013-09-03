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

  private val _keyToRdd: ConcurrentMap[String, RDD[_]] =
    new ConcurrentHashMap[String, RDD[_]]()

  private val _keyToStats: ConcurrentMap[String, collection.Map[Int, TablePartitionStats]] =
    new ConcurrentHashMap[String, collection.Map[Int, TablePartitionStats]]

  def contains(key: String) = _keyToRdd.contains(key.toLowerCase)

  def put(key: String, rdd: RDD[_]) {
    _keyToRdd(key.toLowerCase) = rdd
  }

  def get(key: String): Option[RDD[_]] = _keyToRdd.get(key.toLowerCase)

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
    _keyToRdd.keys.collect { case k: String => k } toSeq
  }

  /**
   * Used to drop an RDD from the Spark in-memory cache and/or disk. All metadata
   * (e.g. entry in '_keyToStats') about the RDD that's tracked by Shark is deleted as well.
   *
   * @param key Used to fetch the an RDD value from '_keyToRDD'.
   * @return Option::isEmpty() is true if there is no RDD value corresponding to 'key' in
   *         '_keyToRDD'. Otherwise, returns a reference to the RDD that was unpersist()'ed.
   */
  def unpersist(key: String): Option[RDD[_]] = {
    def unpersistRDD(rdd: RDD[_]): Unit = {
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
    // Remove RDD's entry from Shark metadata. This also fetches a reference to the RDD object
    // corresponding to the argument for 'key'.
    val rddValue = _keyToRdd.remove(key.toLowerCase())
    _keyToStats.remove(key)
    // Unpersist the RDD using the nested helper fn above.
    rddValue match {
      case Some(rdd) => unpersistRDD(rdd)
      case None => Unit
    }
    rddValue
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
