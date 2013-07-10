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

package shark

import scala.collection.mutable.{ArrayBuffer, Buffer}
import scala.collection.JavaConverters._

import com.google.common.collect.{Ordering => GOrdering}

import spark.RDD
import spark.SparkContext._
import spark.rdd.UnionRDD
import spark.storage.StorageLevel


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

  // TODO(rxin): Move this into execution pacakge.

  def sortLeastKByKey[K <% Ordered[K]: ClassManifest, V: ClassManifest](rdd: RDD[(K,V)], k: Int)
  : RDD[(K,V)] = {
    val partialSortedRdd = partialSortLeastKByKey(rdd, k)
    def createCombiner(v: (K, V)) = ArrayBuffer[(K, V)](v)
    def mergeValue(buf: ArrayBuffer[(K, V)], v: (K, V)) = buf += v
    // Merge k smallest elements of the two arrays
    def mergeCombiners(b1: ArrayBuffer[(K, V)], b2: ArrayBuffer[(K, V)]) = {
      val size = math.min(k, b1.size + b2.size)
      val out = new ArrayBuffer[(K, V)](size)
      var i1 = 0
      var i2 = 0
      for (i <- 0 until size) {
        if (i1 < b1.size && (i2 >= b2.size || b1(i1)._1 <= b2(i2)._1)) {
          out += b1(i1)
          i1 += 1
        } else {
          out += b2(i2)
          i2 += 1
        }
      }
      out
    }
    val combinedRdd = partialSortedRdd.map { (0.toByte, _) }.combineByKey[ArrayBuffer[(K, V)]](
      createCombiner _, mergeValue _, mergeCombiners _, 1)

    combinedRdd.mapPartitions { iter =>
      if (iter.hasNext)
        iter.next._2.toIterator
      else
        Iterator[(K,V)]()
    }
  }

  def partialSortLeastKByKey[K <% Ordered[K]: ClassManifest, V: ClassManifest](
    rdd: RDD[(K,V)],
    k: Int)
  : RDD[(K,V)] = {
    rdd.mapPartitions { iter =>
      sortLeastKByKey(iter.toIterable, k).iterator
    }
  }

  def sortLeastKByKey[K <% Ordered[K]: ClassManifest, V: ClassManifest](
    it: Iterable[(K,V)],
    k: Int)
  : Buffer[(K,V)]  = {
    val ordering = new GOrdering[(K,V)]() {
      override def compare(l: (K,V), r: (K, V)) = (l._1).compare(r._1)
    }
    ordering.leastOf(it.asJava, k).asScala
  }

}
