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

import java.util.{HashMap => JavaHashMap}

import scala.collection.JavaConversions._
import scala.collection.mutable.Map

import org.apache.spark.rdd.RDD


/**
 * A container for table metadata specific to Shark and Spark. Currently, this is a lightweight
 * wrapper around either an RDD or multiple RDDs if the Shark table is Hive-partitioned.
 * Note that a Hive-partition of a table is different from an RDD partition. Each Hive-partition
 * is stored as a subdirectory of the table subdirectory in the warehouse directory
 * (e.g. /user/hive/warehouse). So, every Hive-Partition is loaded into Shark as an RDD.
 *
 * TODO(harvey): It could be useful to make MemoryTable a parent class, and have other table types,
 *               such as HivePartitionedTable or TachyonTable, subclass it. For now, there isn't
 *               too much metadata to track, so it should be okay to have a single MemoryTable.
 */
private[shark]
class MemoryTable(
    val tableName: String,
    val isHivePartitioned: Boolean) {

  // Should only be used if the table is not Hive-partitioned. _tableRDD.isEmpty() is true if the
  // table does not contain any data (e.g. it was created from a CREATE TABLE command, but never
  // populated with data).
  private var _tableRDD: Option[RDD[_]] = None

  // Should only be used if a cached table is Hive-partitioned.
  private val _hivePartitionRDDs: Map[String, RDD[_]] =
    if (isHivePartitioned) { new JavaHashMap[String, RDD[_]]() } else { null }

  private var _cacheMode: CacheType.CacheType = CacheType.NONE

  def cacheMode = _cacheMode

  def cacheMode_= (value: CacheType.CacheType) {
    _cacheMode = value
  }

  def tableRDD: Option[RDD[_]] = {
    assert (
      !isHivePartitioned,
      "Table " + tableName + " is Hive-partitioned. Use MemoryTableDesc::hivePartitionRDDs() " +
      "to get RDDs corresponding to partition columns"
    )
    return _tableRDD
  }

  def tableRDD_= (value: RDD[_]) {
    assert(
      !isHivePartitioned,
      "Table " + tableName + " is Hive-partitioned. Pass in a map of <partition key, RDD> pairs " +
      "the 'hivePartitionRDDs =' setter."
    )
    _tableRDD = Some(value)
  }

  def hivePartitionRDDs: Map[String, RDD[_]] = {
    assert(isHivePartitioned,
           "Table " + tableName + " is not Hive-partitioned. Use tableRDD() to get its RDD.")
    _hivePartitionRDDs
  }
}
