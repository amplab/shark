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

import java.util.concurrent.{ConcurrentHashMap => ConcurrentJavaHashMap}

import scala.collection.JavaConversions._
import scala.collection.mutable.ConcurrentMap

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import shark.execution.RDDUtils


/**
 * A metadata container for partitioned Shark table backed by RDDs.
 *
 * Note that a Hive-partition of a table is different from an RDD partition. Each Hive-partition
 * is stored as a subdirectory of the table subdirectory in the warehouse directory
 * (e.g. '/user/hive/warehouse'). So, every Hive-Partition is loaded into Shark as an RDD.
 */
private[shark]
class PartitionedMemoryTable(
    databaseName: String,
    tableName: String,
    cacheMode: CacheType.CacheType,
    unifiedView: Boolean,
    reloadOnRestart: Boolean)
  extends Table(databaseName, tableName, cacheMode, unifiedView, reloadOnRestart) {

  /**
   * A simple, mutable wrapper for an RDD. This is needed so that a entry maintained by a
   * CachePolicy's underlying data structure, such as the LinkedHashMap for LRUCachePolicy, can be
   * updated without causing an eviction.
   * The value entires for a single key in
   * `_keyToPartitions` and `_cachePolicy` will reference the same RDDValue object.
   */
  class RDDValue(var rdd: RDD[TablePartition])

  // A map from the Hive-partition key to the RDD that contains contents of that partition.
  // The conventional string format for the partition key, 'col1=value1/col2=value2/...', can be
  // computed using MemoryMetadataManager#makeHivePartitionKeyStr()
  private val _keyToPartitions: ConcurrentMap[String, RDDValue] =
    new ConcurrentJavaHashMap[String, RDDValue]()

  // The eviction policy for this table's cached Hive-partitions. An example of how this
  // can be set from the CLI:
  //   `TBLPROPERTIES("shark.partition.cachePolicy", "LRUCachePolicy")`.
  // If 'None', then all partitions will be put in memory.
  private var _cachePolicy: CachePolicy[String, RDDValue] = _

  def containsPartition(partitionKey: String): Boolean = _keyToPartitions.contains(partitionKey)

  def getPartition(partitionKey: String): Option[RDD[TablePartition]] = {
    val rddValueOpt: Option[RDDValue] = _keyToPartitions.get(partitionKey)
    if (rddValueOpt.isDefined) _cachePolicy.notifyGet(partitionKey)
    rddValueOpt.map(_.rdd)
  }

  def putPartition(
      partitionKey: String,
      newRDD: RDD[TablePartition],
      isUpdate: Boolean = false): Option[RDD[TablePartition]] = {
    val rddValueOpt = _keyToPartitions.get(partitionKey)
    val prevRDD: Option[RDD[TablePartition]] = rddValueOpt.map(_.rdd)
    val newRDDValue = new RDDValue(newRDD)
    _keyToPartitions.put(partitionKey, newRDDValue)
    _cachePolicy.notifyPut(partitionKey, newRDDValue)
    prevRDD
  }

  def updatePartition(
      partitionKey: String,
      updatedRDD: RDD[TablePartition]): Option[RDD[TablePartition]] = {
    val rddValueOpt = _keyToPartitions.get(partitionKey)
    val prevRDD: Option[RDD[TablePartition]] = rddValueOpt.map(_.rdd)
    if (rddValueOpt.isDefined) {
      // This is an update of an old value, so update the RDDValue's `rdd` entry.
      // Don't notify the `_cachePolicy`. Assumes that getPartition() has already been called to
      // obtain the value of the previous RDD.
      // An RDD update refers to the RDD created from a transform or union.
      val updatedRDDValue = rddValueOpt.get
      updatedRDDValue.rdd = updatedRDD
    }
    prevRDD
  }

  def removePartition(partitionKey: String): Option[RDD[TablePartition]] = {
    val rddRemoved = _keyToPartitions.remove(partitionKey)
    if (rddRemoved.isDefined) {
      _cachePolicy.notifyRemove(partitionKey)
    }
    rddRemoved.map(_.rdd)
  }

  def setPartitionCachePolicy(cachePolicyStr: String, fallbackMaxSize: Int) {
    // The loadFunc will upgrade the persistence level of the RDD to the preferred storage level.
    val loadFunc: String => RDDValue = (partitionKey: String) => {
      val rddValue = _keyToPartitions.get(partitionKey).get
      rddValue.rdd.persist(StorageLevel.MEMORY_AND_DISK)
      rddValue
    }
    // The evictionFunc will unpersist the RDD.
    val evictionFunc: (String, RDDValue) => Unit = (partitionKey, rddValue) => {
      RDDUtils.unpersistRDD(rddValue.rdd)
    }
    val newPolicy = CachePolicy.instantiateWithUserSpecs[String, RDDValue](
      cachePolicyStr, fallbackMaxSize, loadFunc, evictionFunc)
    _cachePolicy = newPolicy
  }

  def cachePolicy: CachePolicy[String, RDDValue] = _cachePolicy

  /** Returns an immutable view of (partition key -> RDD) mappings to external callers */
  def keyToPartitions: collection.immutable.Map[String, RDD[TablePartition]] = {
    _keyToPartitions.mapValues(_.rdd).toMap
  }

}
