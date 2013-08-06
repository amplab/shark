package shark.memstore2.column

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import java.nio.ByteBuffer
import java.nio.ByteOrder
/**
 * Handles null values.
 */
trait NullableColumnBuilder[T] extends ColumnBuilder[T] {

  private var _nulls: ByteBuffer = _
  private var _pos: Int = 0
  private var _nullCount = 0
  override def initialize(initialSize: Int) {
    _nulls = ByteBuffer.allocate(1024)
    //first 4 bytes indicate the # of nulls to expect.
    _nulls.order(ByteOrder.nativeOrder())
    _pos = 0
    super.initialize(initialSize)
  }

  override def append(o: Object, oi: ObjectInspector): Unit = {
    if (o == null) {
      _nulls = growIfNeeded(_nulls)
      _nulls.putInt(_pos)
      _nullCount += 1
    } else {
      super.append(o, oi)
    }
    _pos += 1
  }

  override def build: ByteBuffer = {
    val b = super.build
    if (_pos == 0) {
      b
    } else {
      val v = _nulls.position()
      _nulls.limit(v)
      _nulls.rewind()
      val newBuffer = ByteBuffer.allocate(b.limit + v + 4)
      newBuffer.order(ByteOrder.nativeOrder())
      val colType= b.getInt()
      newBuffer.putInt(colType).putInt(_nullCount).put(_nulls).put(b)
      newBuffer.rewind()
      newBuffer
    }
  }
}