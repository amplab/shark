package shark.memstore2.column

import java.nio.ByteBuffer
import java.nio.ByteOrder

/**
 * Reads a nullable column. Expects the byte buffer to contain as first element
 * the null count, followed by the null indices, and finally the non nulls.
 * Reading of non nulls is delegated by setting the buffer position to the first
 * non null.
 */
class NullableColumnIterator(delegate: ColumnIterator, buffer: ByteBuffer) extends ColumnIterator {
  private var _d: ByteBuffer = _
  private var _nullCount: Int = _
  private var _nulls = 0
 
  private var _isNull = false
  private var _currentNullIndex:Int = _
  private var _pos = 0
  
  override def init() {
    _d = buffer.duplicate()
    _d.order(ByteOrder.nativeOrder())
    _nullCount = _d.getInt()
    buffer.position(buffer.position() + _nullCount*4 + 4)
    _currentNullIndex = if (_nullCount > 0) _d.getInt() else Integer.MAX_VALUE
    _pos = 0
    delegate.init()
  }

  override def computeNext() {
    if (_pos == _currentNullIndex) {
      _nulls += 1
      if (_nulls < _nullCount) {
        _currentNullIndex = _d.getInt()
      }
      _isNull = true
    } else {
      _isNull = false
      delegate.computeNext()
    }
    _pos += 1
  }
  
  def current(): Object = {
    if (_isNull) null else delegate.current
  }
}