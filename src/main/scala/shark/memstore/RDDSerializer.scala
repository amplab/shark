package shark.memstore

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.serde2.SerDe
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.io.Writable


/**
 * Serialize a RDD partition using the given columnar serde.
 * Make sure the serde is initialized before passing it in.
 */
class RDDSerializer(serDe: ColumnarSerDe) {
  // Columnar serialization serializes a partition into two elements. The first
  // element is the number of rows in the partition, and the second element is
  // a ColumnarWritable object containing all rows in columnar format.

  def serialize(iter: Iterator[Any], oi: ObjectInspector): Iterator[_] = {

    var tableStorageBuilder: Writable = null
    iter.foreach { row =>
      tableStorageBuilder = serDe.serialize(row.asInstanceOf[AnyRef], oi)
    }

    if (tableStorageBuilder != null) {
      Iterator(tableStorageBuilder.asInstanceOf[TableStorageBuilder].build)
    } else {
      // This partition is empty.
      Iterator()
    }
  }
}
