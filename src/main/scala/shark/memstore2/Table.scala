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

import org.apache.spark.storage.StorageLevel


/**
 * A container for table metadata specific to Shark and Spark. Currently, this is a lightweight
 * wrapper around either an RDD or multiple RDDs if the Shark table is Hive-partitioned.
 *
 * Note that a Hive-partition of a table is different from an RDD partition. Each Hive-partition
 * is stored as a subdirectory of the table subdirectory in the warehouse directory
 * (e.g. '/user/hive/warehouse'). So, every Hive-Partition is loaded into Shark as an RDD.
 */
private[shark] abstract class Table(
    var tableName: String,
    var cacheMode: CacheType.CacheType,
    var preferredStorageLevel: StorageLevel) {

  def getPreferredStorageLevel: StorageLevel

  def getCurrentStorageLevel: StorageLevel
}
