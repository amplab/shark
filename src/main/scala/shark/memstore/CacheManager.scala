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

package shark.memstore

import spark.RDD
import spark.storage.StorageLevel

class CacheManager {

  private val _keyToRdd = new collection.mutable.HashMap[String, RDD[_]]()

  private val _keyToStats = new collection.mutable.HashMap[String, collection.Map[Int, TableStats]]

  def put(key: String, rdd: RDD[_], storageLevel: StorageLevel) {
    _keyToRdd(key.toLowerCase) = rdd
    rdd.persist(storageLevel)
  }

  def get(key: String): Option[RDD[_]] = _keyToRdd.get(key.toLowerCase)

  def putStats(key: String, stats: collection.Map[Int, TableStats]) {
    _keyToStats.put(key.toLowerCase, stats)
  }

  def getStats(key: String): Option[collection.Map[Int, TableStats]] = {
    _keyToStats.get(key.toLowerCase)
  }

  /**
   * Find all keys that are strings. Used to drop tables after exiting.
   */
  def getAllKeyStrings(): Seq[String] = {
    _keyToRdd.keys.collect { case k: String => k } toSeq
  }
}

object CacheManager {
  /**
   * Return a StorageLevel corresponding to its String name.
   */
   def matchStringWithStorageLevel(storageLevelStr: String): StorageLevel = {
     storageLevelStr.toUpperCase match {
        case "NONE" => StorageLevel.NONE
        case "DISK_ONLY" => StorageLevel.DISK_ONLY
        case "DISK_ONLY2" => StorageLevel.DISK_ONLY_2
        case "MEMORY_ONLY" => StorageLevel.MEMORY_ONLY
        case "MEMORY_ONLY_2" => StorageLevel.MEMORY_ONLY_2
        case "MEMORY_ONLY_SER" => StorageLevel.MEMORY_ONLY_SER
        case "MEMORY_ONLY_SER2" => StorageLevel.MEMORY_ONLY_SER_2
        case "MEMORY_AND_DISK" => StorageLevel.MEMORY_AND_DISK
        case "MEMORY_AND_DISK_2" => StorageLevel.MEMORY_AND_DISK_2
        case "MEMORY_AND_DISK_SER" => StorageLevel.MEMORY_AND_DISK_SER
        case "MEMORY_AND_DISK_SER_2" => StorageLevel.MEMORY_AND_DISK_SER_2
        case _ => throw new IllegalArgumentException(
          "Unrecognized storage level: " + storageLevelStr)
      }
   }
}
