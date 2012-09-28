package shark.memstore

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.serde2.SerDe
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector


trait RDDSerializer {
  def serialize[T](iter: Iterator[T], oi: ObjectInspector): Iterator[_]
  def deserialize(iter: Iterator[_]): Iterator[_]
}


object RDDSerializer {
  /**
   * Serialize a RDD partition using the given columnar serde.
   * Make sure the serde is initialized before passing it in.
   */
  class Columnar(serDe: SerDe) extends RDDSerializer {
    // Columnar serialization serializes a partition into two elements. The first
    // element is the number of rows in the partition, and the second element is
    // a ColumnarWritable object containing all rows in columnar format.

    override def serialize[T](iter: Iterator[T], oi: ObjectInspector) : Iterator[_] = {
      var v: Object = null
      var numRows: Int = 0
      iter.foreach { row =>
        v = serDe.serialize(row, oi)
        numRows += 1
      }
      if (v != null) {
        v.asInstanceOf[ColumnarWritable].close
        Iterator(numRows, v)
      } else {
        // This partition is empty.
        Iterator()
      }
    }

    override def deserialize(iter: Iterator[_]): Iterator[_] = {
      if (iter.hasNext) {
        // For non-empty partitions, the first element is the number of rows,
        // and the second element is a writable object containing all rows.
        val numRows = iter.next().asInstanceOf[Int]
        val writable = iter.next().asInstanceOf[ColumnarWritable]
        writable.close
        Iterator.continually({writable}).take(numRows) //Reuses writable across rows
      } else {
        // This partition is empty. Simply return the empty iterator.
        iter
      }
    }
  }
}
