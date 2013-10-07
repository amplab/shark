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
 * A container for table metadata specific to Shark and Spark. Currently, this is a lightweight
 * wrapper around either an RDD or multiple RDDs if the Shark table is Hive-partitioned.
 * Note that a Hive-partition of a table is different from an RDD partition. Each Hive-partition
 * is stored as a subdirectory of the table subdirectory in the warehouse directory
 * (e.g. /user/hive/warehouse). So, every Hive-Partition is loaded into Shark as an RDD.
 */
private[shark] abstract class Table(
    var tableName: String,
    var cacheMode: CacheType.CacheType) {
  def getStorageLevel: StorageLevel
}

private[shark]
class MemoryTable(
    tableName: String,
    cacheMode: CacheType.CacheType)
  extends Table(tableName, cacheMode) {

  // RDD that contains the contents of this table.
  var tableRDD: RDD[_] = _

  override def getStorageLevel: StorageLevel = RDDUtils.getStorageLevelOfCachedRDD(tableRDD)
}

private[shark]
class PartitionedMemoryTable(
    tableName: String,
    cacheMode: CacheType.CacheType)
  extends Table(tableName, cacheMode) {

  // A map from the Hive-partition key to the RDD that contains contents of that partition.
  private var _keyToPartitions: ConcurrentMap[String, RDD[_]] =
    new ConcurrentJavaHashMap[String, RDD[_]]()

  // The eviction policy for this table's cached Hive-partitions. An example of how this
  // can be set from the CLI:
  //   'TBLPROPERTIES("shark.partition.cachePolicy", "LRUCachePolicy")'.
  private var _cachePolicy: CachePolicy[String, RDD[_]] = _

  private var _cachePolicyName: String = "None"

  def containsPartition(partitionKey: String): Boolean = _keyToPartitions.contains(partitionKey)

  def getPartition(partitionKey: String): Option[RDD[_]] = {
    val rddFound = _keyToPartitions.get(partitionKey)
    if (rddFound.isDefined) _cachePolicy.notifyGet(partitionKey)
    return rddFound
  }

  def putPartition(partitionKey: String, rdd: RDD[_]): Option[RDD[_]] = {
    _cachePolicy.notifyPut(partitionKey, rdd)
    _keyToPartitions.put(partitionKey, rdd)
  }

  def removePartition(partitionKey: String): Option[RDD[_]] = {
    val rddRemoved = _keyToPartitions.remove(partitionKey)
    if (rddRemoved.isDefined) _cachePolicy.notifyRemove(partitionKey, rddRemoved.get)
    return rddRemoved
  }

  def setPartitionCachePolicy(
      cachePolicyStr: String,
      maxSize: Long,
      shouldRecordStats: Boolean
    ) {
    _cachePolicy =
      Class.forName(cachePolicyStr).newInstance.asInstanceOf[CachePolicy[String, RDD[_]]]
    val loadFunc: String => RDD[_] =
      (partitionKey: String) => {
        val partitionRDD = _keyToPartitions.get(partitionKey).get
        partitionRDD.persist(RDDUtils.getStorageLevelOfCachedRDDs(getAllPartitions))
        partitionRDD
      }
    val evictionFunc: (String, RDD[_]) => Unit =
      (partitionKey: String, partition: RDD[_]) => partition.unpersist()
    _cachePolicy.initialize(maxSize, loadFunc, evictionFunc, shouldRecordStats)
    _cachePolicyName = cachePolicyStr
  }

  def cachePolicyName: String = _cachePolicyName

  def cachePolicy: CachePolicy[String, RDD[_]] = _cachePolicy

  def getAllPartitions = _keyToPartitions.values.toSeq

  def getAllPartitionKeys = _keyToPartitions.keys.toSeq

  def getStorageLevel: StorageLevel = RDDUtils.getStorageLevelOfCachedRDDs(getAllPartitions)
}

