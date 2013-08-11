package shark.memstore2.column

import java.nio.ByteBuffer
import shark.memstore2.column._
import shark.memstore2.column.Implicits._
import java.nio.ByteOrder

/**
 * Iterates through a byte buffer containing compressed data.
 * The first element of the buffer at the point of initialization
 * is expected to be the type of compression indicator.
 * 
 */
trait CompressedColumnIterator extends ColumnIterator{

  def buffer: ByteBuffer
  def columnType: ColumnType[_,_]
  private var _compressionType: CompressionType = _
  private var _decoder: Iterator[_] = _
  private var _current: Any = _
  override def init() {
    _compressionType = buffer.getInt()
    _decoder = _compressionType match {
      case DEFAULT => new DefaultDecoder(buffer, columnType)
      case RLECompressionType => new RLDecoder(buffer, columnType)
      case _ => throw new UnsupportedOperationException()
    }
  }

  override def computeNext() {
    if (_decoder.hasNext) {
      _current = _decoder.next
    }
  }
  
  override def current = _current.asInstanceOf[Object]
}

/**
 * Default representation of a Decoder. In this case the underlying buffer
 * has uncompressed data
 */
class DefaultDecoder[V](buffer:ByteBuffer, columnType: ColumnType[_, V]) extends Iterator[V] {
  private var _current: V = columnType.newWritable
  override def hasNext() = buffer.hasRemaining()
  override def next():V = {
    columnType.extractInto(buffer.position(), buffer, _current)
    _current
  }
}

/**
 * Run Length Decoder, decodes data compressed in RLE format of [element, length]
 */
class RLDecoder[V](buffer:ByteBuffer, columnType: ColumnType[_, V]) extends Iterator[V] {
  
  private var _run: Int = _
  private var _count: Int = 0
  private var _current: V = columnType.newWritable

  override def hasNext() = buffer.hasRemaining()
  
  override def next():V = {
    if (_count == _run) {
      //next run
      columnType.extractInto(buffer.position(), buffer, _current)
      _run = buffer.getInt()
      _count = 1
      
    } else {
      _count += 1
    }
    _current
  }
}