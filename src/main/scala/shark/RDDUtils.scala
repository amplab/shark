package shark

import com.google.common.collect.{Ordering => GOrdering}

import scala.collection.mutable.{ArrayBuffer, Buffer}
import scala.collection.JavaConverters._

import spark.RDD
import spark.SparkContext._


/**
 * A set of RDD-related functions that provide some handy features in addition
 * to Spark's built-in abstractions.
 */
object RDDUtils {

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
