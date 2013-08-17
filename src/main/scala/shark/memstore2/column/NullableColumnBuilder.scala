package shark.memstore2.column

import java.nio.ByteBuffer
import java.nio.ByteOrder

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector


/**
 * Builds a nullable column. The byte buffer of a nullable column contains
 * the column type, followed by the null count and the index of nulls, followed
 * finally by the non nulls.
 */
trait NullableColumnBuilder[T] extends ColumnBuilder[T] {

  private var _nulls: ByteBuffer = _
  
  private var _pos: Int = _
  private var _nullCount:Int = _

  override def initialize(initialSize: Int): ByteBuffer = {
    _nulls =  ByteBuffer.allocate(1024)
    _nulls.order(ByteOrder.nativeOrder())
    _pos = 0
    _nullCount = 0
    super.initialize(initialSize)
  }

  override def append(o: Object, oi: ObjectInspector) {
    if (o == null) {
      _nulls = growIfNeeded(_nulls, 4)
      _nulls.putInt(_pos)
      _nullCount += 1
    } else {
      super.append(o, oi)
    }
    _pos += 1
  }

  override def build(): ByteBuffer = {
    val b = super.build()
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