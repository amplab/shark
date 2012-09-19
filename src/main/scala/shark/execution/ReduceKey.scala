package shark.execution

import java.util.Arrays
import org.apache.hadoop.io.WritableComparator
import scala.collection.mutable.StringBuilder
import spark.HashPartitioner


/**
 * A data structure used for shuffling data that supports comparison. We wrap
 * ReduceKey around a normal key byte array so the byte array can be used in
 * binary comparison, as well as enabling partitioning data based on the
 * partitionCode field using a ReduceKeyPartitioner.
 */
class ReduceKey(val bytes: Array[Byte]) extends Serializable with Ordered[ReduceKey] {

  override def hashCode(): Int = {
    Arrays.hashCode(bytes)
  }

  /** Used by ReduceKeyPartitioner to determine the hash partition. */
  @transient var partitionCode = 0

  override def equals(other: Any): Boolean  = {
    other match {
      case other: ReduceKey => Arrays.equals(bytes, other.bytes)
      case _ => false
    }
  }
  
  def compareBytes(a: Array[Byte], b: Array[Byte]): Int = {
    WritableComparator.compareBytes(a, 0, a.length, b, 0, b.length)
  }

  override def compare(that: ReduceKey): Int = {
    compareBytes(this.bytes, that.bytes)
  }

  override def toString() = {
    var s = StringBuilder.newBuilder
    bytes.foreach { b => s.append(" " + String.valueOf(b.toInt) + " ") }
    s.toString
  }
}


/**
 * A special Spark partitioner that allows hash partitioning of data based on
 * the partitionCode field in ReduceKey.
 */
class ReduceKeyPartitioner(partitions: Int) extends HashPartitioner(partitions) {

  override def getPartition(key: Any): Int = {
    key match {
      case k: ReduceKey => {
        val mod = k.partitionCode % partitions
        if (mod < 0) mod + partitions else mod  // Guard against negative hash codes.
      }
      case other => {
        throw new Exception(
          "ReduceKeyPartitioner expects object of class ReduceKey, but got " +
          other.getClass.getName)
      }
    }
  }
  
}
