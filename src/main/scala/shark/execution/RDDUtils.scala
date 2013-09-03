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

package shark.execution

import scala.collection.JavaConversions

import com.google.common.collect.{Ordering => GOrdering}

import org.apache.spark.{HashPartitioner, Partitioner, RangePartitioner}
import org.apache.spark.rdd.{RDD, ShuffledRDD, UnionRDD}
import org.apache.spark.storage.StorageLevel

import shark.SharkEnv


/**
 * A set of RDD-related functions that provide some handy features in addition
 * to Spark's built-in abstractions.
 */
object RDDUtils {

  def getStorageLevelOfCachedTable(rdd: RDD[_]): StorageLevel = {
    rdd match {
      case u: UnionRDD[_] => u.rdds.foldLeft(rdd.getStorageLevel) {
        (s, r) => {
          if (s == StorageLevel.NONE) {
            getStorageLevelOfCachedTable(r)
          } else {
            s
          }
        }
      }
      case _ => rdd.getStorageLevel
    }
  }

  /**
   * Repartition an RDD using the given partitioner. This is similar to Spark's partitionBy,
   * except we use the Shark shuffle serializer.
   */
  def repartition[K: ClassManifest, V: ClassManifest](rdd: RDD[(K, V)], part: Partitioner)
    : RDD[(K, V)] =
  {
    new ShuffledRDD[K, V, (K, V)](rdd, part).setSerializer(SharkEnv.shuffleSerializerName)
  }

  /**
   * Sort the RDD by key. This is similar to Spark's sortByKey, except that we use
   * the Shark shuffle serializer.
   */
  def sortByKey[K <: Comparable[K]: ClassManifest, V: ClassManifest](rdd: RDD[(K, V)])
    : RDD[(K, V)] =
  {
    val part = new RangePartitioner(rdd.partitions.length, rdd)
    val shuffled = new ShuffledRDD[K, V, (K, V)](rdd, part)
      .setSerializer(SharkEnv.shuffleSerializerName)
    shuffled.mapPartitions(iter => {
      val buf = iter.toArray
      buf.sortWith((x, y) => x._1.compareTo(y._1) < 0).iterator
    }, true)
  }

  /**
   * Return an RDD containing the top K (K smallest key) from the given RDD.
   */
  def topK[K <: Comparable[K]: ClassManifest, V: ClassManifest](rdd: RDD[(K, V)], k: Int)
    : RDD[(K, V)] =
  {
    // First take top K on each partition.
    val partialSortedRdd = partitionTopK(rdd, k)
    // Then merge all partitions into a single one, and take the top K on that partition.
    partitionTopK(repartition(partialSortedRdd, new HashPartitioner(1)), k)
  }

  /**
   * Take top K on each partition and return a new RDD.
   */
  def partitionTopK[K <: Comparable[K]: ClassManifest, V: ClassManifest](
    rdd: RDD[(K, V)], k: Int): RDD[(K, V)] = {
    rdd.mapPartitions(iter => topK(iter, k))
  }

  /**
   * Return top K elements out of an iterator.
   */
  private def topK[K <: Comparable[K]: ClassManifest, V: ClassManifest](
    it: Iterator[(K, V)], k: Int): Iterator[(K, V)]  = {
    val ordering = new GOrdering[(K,V)] {
      override def compare(l: (K, V), r: (K, V)) = {
        (l._1).compareTo(r._1)
      }
    }
    // Guava only takes Java iterators. Convert the iterator into Java iterator and then
    // convert it back to Scala.
    JavaConversions.asScalaIterator(
      ordering.leastOf(JavaConversions.asJavaIterator(it), k).iterator)
  }

}
