package shark.memstore2.column

import java.nio.ByteBuffer
import java.nio.ByteOrder

/**
 * 
 */
class NullableColumnIterator(delegate: ColumnIterator, buffer: ByteBuffer)
  extends ColumnIterator{
  private val d = buffer.duplicate()
  d.order(ByteOrder.nativeOrder())
  private val nullCount = d.getInt()
  private var nulls = 0
 
  buffer.position(buffer.position() + nullCount*4 + 4)
  private var isNull = false
  private var currentNullIndex = if (nullCount > 0) d.getInt() else Integer.MAX_VALUE
  private var pos = 0
  
  override def next() {
    if (pos == currentNullIndex) {
      nulls += 1
      if (nulls < nullCount) {
        currentNullIndex = d.getInt()
      }
      isNull = true
    } else {
      isNull = false
      delegate.next()
    }
    pos += 1
  }
  
  def current: Object = {
    if (isNull) null else delegate.current
  }
}