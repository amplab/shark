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

import org.apache.spark.rdd.RDD

import scala.collection.mutable.{Buffer, HashMap}

import shark.execution.RDDUtils


/**
 * A metadata container for a table in Shark that's backed by an RDD.
 */
private[shark] class MemoryTable(
    databaseName: String,
    tableName: String,
    cacheMode: CacheType.CacheType)
  extends Table(databaseName, tableName, cacheMode) {

  private var _rddValueOpt: Option[RDDValue] = None

  /**
   * Sets the RDD and stats fields the `_rddValueOpt`. Used for INSERT/LOAD OVERWRITE.
   * @param newRDD The table's data.
   * @param newStats Stats for each TablePartition in `newRDD`.
   * @return The previous (RDD, stats) pair for this table.
   */
  def put(
  	  newRDD: RDD[TablePartition],
  	  newStats: collection.Map[Int, TablePartitionStats] = new HashMap[Int, TablePartitionStats]()
  	): Option[(RDD[TablePartition], collection.Map[Int, TablePartitionStats])] = {
  	val prevRDDAndStatsOpt = _rddValueOpt.map(_.toTuple)
  	if (_rddValueOpt.isDefined) {
  	  _rddValueOpt.foreach { rddValue =>
  	  	rddValue.rdd = newRDD
  	  	rddValue.stats = newStats
  	  }
  	} else {
      _rddValueOpt = Some(new RDDValue(newRDD, newStats))
  	}
    prevRDDAndStatsOpt 
  }

  /**
   * Used for append operations, such as INSERT and LOAD INTO.
   *
   * @param newRDD Data to append to the table.
   * @param newStats Stats for each TablePartition in `newRDD`.
   * @return The previous (RDD, stats) pair for this table.
   */
  def update(
  	  newRDD: RDD[TablePartition],
  	  newStats: Buffer[(Int, TablePartitionStats)]
  	): Option[(RDD[TablePartition], collection.Map[Int, TablePartitionStats])] = {
    val prevRDDAndStatsOpt = _rddValueOpt.map(_.toTuple)
  	if (_rddValueOpt.isDefined) {
      val (prevRDD, prevStats) = (prevRDDAndStatsOpt.get._1, prevRDDAndStatsOpt.get._2)
      val updatedRDDValue = _rddValueOpt.get
      updatedRDDValue.rdd = RDDUtils.unionAndFlatten(newRDD, prevRDD)
      updatedRDDValue.stats = Table.mergeStats(newStats, prevStats).toMap
    } else {
      put(newRDD, newStats.toMap)
    }
    prevRDDAndStatsOpt
  }

  def getRDD = _rddValueOpt.map(_.rdd)

  def getStats = _rddValueOpt.map(_.stats)

}
