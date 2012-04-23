package shark.exec

import java.util.Arrays
import org.apache.hadoop.io.WritableComparator
import scala.collection.mutable.StringBuilder

class ReduceKey(val bytes: Array[Byte]) extends Serializable with Ordered[ReduceKey] {

  override def hashCode(): Int = {
    Arrays.hashCode(bytes)
  }

  override def equals(other: Any): Boolean  = {
    other match {
      case other: ReduceKey =>
        Arrays.equals(bytes, other.bytes)
      case _ =>
        false
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
