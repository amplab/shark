package shark.memstore2.column

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import java.nio.ByteBuffer
import java.nio.ByteOrder
/**
 * Handles null values.
 */
trait NullableColumnBuilder[T] extends ColumnBuilder[T] {

  private var nulls: ByteBuffer = _
  private var pos: Int = 0
  private var nullCount = 0
  override def initialize(initialSize: Int) {
    nulls = ByteBuffer.allocate(1024)
    //first 4 bytes indicate the # of nulls to expect.
    nulls.order(ByteOrder.nativeOrder())
    pos = 0
    super.initialize(initialSize)
  }

  override def append(o: Object, oi: ObjectInspector): Unit = {
    if (o == null) {
      nulls = growIfNeeded(nulls)
      nulls.putInt(pos)
      nullCount += 1
    } else {
      super.append(o, oi)
    }
    pos += 1
  }

  override def build: ByteBuffer = {
    val b = super.build
    if (pos == 0) {
      b
    } else {
      val v = nulls.position()
      nulls.limit(v)
      nulls.rewind()
      val newBuffer = ByteBuffer.allocate(b.limit + v + 4)
      newBuffer.order(ByteOrder.nativeOrder())
      val colType= b.getInt()
      newBuffer.putInt(colType).putInt(nullCount).put(nulls).put(b)
      newBuffer.rewind()
      newBuffer
    }
  }
}