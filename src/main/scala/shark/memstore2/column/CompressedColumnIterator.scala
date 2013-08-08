package shark.memstore2.column

import java.nio.ByteBuffer
import shark.memstore2.column._
import shark.memstore2.column.Implicits._
import java.nio.ByteOrder

trait CompressedColumnIterator extends ColumnIterator{

  def buffer: ByteBuffer
  private val _d = buffer.duplicate
  _d.order(ByteOrder.nativeOrder())
  private val _compressionType: CompressionType = _d.getInt()
  private val _columnType: ColumnType[_] = _d.getInt()
  private val _decoder: Iterator[_] = _compressionType match {
    case RLECompressionType => new RLDecoder(_d, _columnType)
    case _ => throw new UnsupportedOperationException()
  }
  private var _current: Any = _
  override def next() {
    if (_decoder.hasNext) {
      _current = _decoder.next
    }
  }
  
  override def current = _current.asInstanceOf[Object]
}

class RLDecoder[T](buffer:ByteBuffer, columnType: ColumnType[T]) extends Iterator[T] {
  
  private var _run: Int = _
  private var _count: Int = 0
  private var _current: T = _

  override def hasNext() = buffer.hasRemaining()
  
  override def next():T = {
    if (_count == _run) {
      //next run
      _current = columnType.extract(buffer.position(), buffer)
      _run = buffer.getInt()
      _count = 1
      
    } else {
      _count += 1
    }
    _current
  }
}