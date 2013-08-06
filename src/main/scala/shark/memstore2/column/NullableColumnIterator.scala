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
  private val _nullCount = d.getInt()
  private var _nulls = 0
 
  buffer.position(buffer.position() + _nullCount*4 + 4)
  private var _isNull = false
  private var _currentNullIndex = if (_nullCount > 0) d.getInt() else Integer.MAX_VALUE
  private var _pos = 0
  
  override def next() {
    if (_pos == _currentNullIndex) {
      _nulls += 1
      if (_nulls < _nullCount) {
        _currentNullIndex = d.getInt()
      }
      _isNull = true
    } else {
      _isNull = false
      delegate.next()
    }
    _pos += 1
  }
  
  def current: Object = {
    if (_isNull) null else delegate.current
  }
}