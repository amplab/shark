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

  val keyToRdd = new collection.mutable.HashMap[CacheKey, RDD[_]]()

  val keyToStats = new collection.mutable.HashMap[CacheKey, collection.Map[Int, TableStats]]

  def put(key: CacheKey, rdd: RDD[_], storageLevel: StorageLevel) {
    keyToRdd(key) = rdd
    rdd.persist(storageLevel)
  }

  def get(key: CacheKey): Option[RDD[_]] = keyToRdd.get(key)

  /**
   * Find all keys that are strings. Used to drop tables after exiting.
   */
  def getAllKeyStrings(): Seq[String] = {
    keyToRdd.keys.map(_.key).collect { case k: String => k } toSeq
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
