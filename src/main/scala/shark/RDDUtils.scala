package shark

import com.google.common.collect.{Ordering => GOrdering}

import java.io.DataInputStream
import java.io.DataOutputStream

import it.unimi.dsi.fastutil.io.FastByteArrayInputStream
import it.unimi.dsi.fastutil.io.FastByteArrayOutputStream

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.plan.TableDesc
import org.apache.hadoop.hive.serde2.SerDe
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.io.Text

import scala.collection.mutable.{ArrayBuffer, Buffer}
import scala.collection.JavaConverters._

import spark._
import spark.SparkContext._


/**
 * A set of RDD-related functions that provide some handy features in addition
 * to Spark's built-in abstractions.
 */
object RDDUtils {
  
  /**
   * Used to serialize an RDD using a Hive SerDe before caching in a
   * BoundedMemoryCache. We serialize this way instead of using a
   * SerializingCache because this requires less copying of byte arrays.
   */
  def serialize[T](iter: Iterator[T], hconf: HiveConf, td: TableDesc, oi: ObjectInspector) = {
    val serializer = td.getDeserializerClass().newInstance().asInstanceOf[SerDe]
    serializer.initialize(hconf, td.getProperties())
    var v: Object = null
    var numRows: Int = 0
    iter.foreach { row =>
      v = serializer.serialize(row, oi)
      numRows += 1
    }
    if (v != null) {
      v.asInstanceOf[ColumnarWritable].close()
      Iterator(numRows, v)
    } else {
      Iterator()
    }
  }

  def deserialize(rdd: RDD[_]) = {    
    rdd.mapPartitions { iter =>
      //First element is a byte array holding serialized Writables
      if (iter.hasNext) {
        val numRows = iter.next().asInstanceOf[Int]
        val writable = iter.next().asInstanceOf[ColumnarWritable]
        writable.close()
        Iterator.continually({writable}).take(numRows) //Reuses writable across rows
      } else {
        iter
      }
    }
  }

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
        }
        else {
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
