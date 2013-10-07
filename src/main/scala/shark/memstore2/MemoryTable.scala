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

import org.apache.spark.rdd.{EmptyRDD, RDD}
import org.apache.spark.storage.StorageLevel

import shark.execution.RDDUtils


/**
 * A container for table metadata specific to Shark and Spark. Currently, this is a lightweight
 * wrapper around either an RDD or multiple RDDs if the Shark table is Hive-partitioned.
 * Note that a Hive-partition of a table is different from an RDD partition. Each Hive-partition
 * is stored as a subdirectory of the table subdirectory in the warehouse directory
 * (e.g. /user/hive/warehouse). So, every Hive-Partition is loaded into Shark as an RDD.
 */
private[shark] abstract class Table(
    var tableName: String,
    var cacheMode: CacheType.CacheType,
    var preferredStorageLevel: StorageLevel) {

  def getPreferredStorageLevel: StorageLevel

  def getCurrentStorageLevel: StorageLevel
}

private[shark]
class MemoryTable(
    tableName: String,
    cacheMode: CacheType.CacheType,
    preferredStorageLevel: StorageLevel)
  extends Table(tableName, cacheMode, preferredStorageLevel) {

  // RDD that contains the contents of this table.
  private var _tableRDD: RDD[_] = _

  def tableRDD: RDD[_] = _tableRDD

  def tableRDD_= (rdd: RDD[_]) = _tableRDD = rdd

  override def getPreferredStorageLevel: StorageLevel = preferredStorageLevel

  override def getCurrentStorageLevel: StorageLevel = RDDUtils.getStorageLevelOfRDD(tableRDD)
}

private[shark]
class PartitionedMemoryTable(
    tableName: String,
    cacheMode: CacheType.CacheType,
    preferredStorageLevel: StorageLevel)
  extends Table(tableName, cacheMode, preferredStorageLevel) {

  private class RDDValue(var rdd: RDD[_])

  // A map from the Hive-partition key to the RDD that contains contents of that partition.
  private var _keyToPartitions: ConcurrentMap[String, RDDValue] =
    new ConcurrentJavaHashMap[String, RDDValue]()

  // The eviction policy for this table's cached Hive-partitions. An example of how this
  // can be set from the CLI:
  //   'TBLPROPERTIES("shark.partition.cachePolicy", "LRUCachePolicy")'.
  private var _cachePolicy: CachePolicy[String, RDDValue] = _

  private var _cachePolicyName: String = "None"

  def containsPartition(partitionKey: String): Boolean = _keyToPartitions.contains(partitionKey)

  def getPartition(partitionKey: String): Option[RDD[_]] = {
    val rddValueFound = _keyToPartitions.get(partitionKey)
    if (rddValueFound.isDefined) _cachePolicy.notifyGet(partitionKey)
    return rddValueFound.map(_.rdd)
  }

  def updatePartition(partitionKey: String, newRDD: RDD[_]): Option[RDD[_]] = {
    val rddValueFound = _keyToPartitions.get(partitionKey)
    var oldRDD: Option[RDD[_]] = None
    if (rddValueFound.isDefined) {
      val reusedRDDValue = rddValueFound.get
      oldRDD = Some(reusedRDDValue.rdd)
      reusedRDDValue.rdd = newRDD
      reusedRDDValue
    }
    return oldRDD
  }

  def putPartition(partitionKey: String, newRDD: RDD[_]): Option[RDD[_]] = {
    val rddValueFound = _keyToPartitions.get(partitionKey)
    var oldRDD: Option[RDD[_]] = rddValueFound.map(_.rdd)
    val newRDDValue = new RDDValue(newRDD)
    _keyToPartitions.put(partitionKey, newRDDValue)
    _cachePolicy.notifyPut(partitionKey, newRDDValue)
    return oldRDD
  }

  def removePartition(partitionKey: String): Option[RDD[_]] = {
    val rddRemoved = _keyToPartitions.remove(partitionKey)
    if (rddRemoved.isDefined) _cachePolicy.notifyRemove(partitionKey, rddRemoved.get)
    return rddRemoved.map(_.rdd)
  }

  def setPartitionCachePolicy(
      cachePolicyStr: String,
      maxSize: Long,
      shouldRecordStats: Boolean
    ) {
    _cachePolicy =
      Class.forName(cachePolicyStr).newInstance.asInstanceOf[CachePolicy[String, RDDValue]]
    val loadFunc: String => RDDValue =
      (partitionKey: String) => {
        val rddValue = _keyToPartitions.get(partitionKey).get
        if (RDDUtils.getStorageLevelOfRDD(rddValue.rdd) == StorageLevel.NONE) {
          rddValue.rdd.persist(preferredStorageLevel)
        }
        rddValue
      }
    val evictionFunc: (String, RDDValue) => Unit =
      (partitionKey: String, rddValue) => RDDUtils.unpersistRDD(rddValue.rdd)
    _cachePolicy.initialize(maxSize, loadFunc, evictionFunc, shouldRecordStats)
    _cachePolicyName = cachePolicyStr
  }

  def cachePolicyName: String = _cachePolicyName

  def cachePolicy: CachePolicy[String, _] = _cachePolicy

  def keyToPartitions: collection.immutable.Map[String, RDD[_]] = {
    return _keyToPartitions.mapValues(_.rdd).toMap
  }

  override def getPreferredStorageLevel: StorageLevel = preferredStorageLevel

  override def getCurrentStorageLevel: StorageLevel = {
    return RDDUtils.getStorageLevelOfRDDs(_keyToPartitions.values.map(_.rdd).toSeq)
  }
}
