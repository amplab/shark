package shark.memstore2.column

import java.nio.ByteBuffer

import shark.memstore2.column.Implicits._

/**
 * Iterates through a byte buffer containing compressed data.
 * The first element of the buffer at the point of initialization
 * is expected to be the type of compression indicator.
 */
trait CompressedColumnIterator extends ColumnIterator {

  private var _decoder: Iterator[_] = _
  private var _current: Any = _

  def buffer: ByteBuffer

  def columnType: ColumnType[_,_]

  override def init() {
    val compressionType: CompressionType = buffer.getInt()
    _decoder = compressionType match {
      case DefaultCompressionType => new DefaultDecoder(buffer, columnType)
      case RLECompressionType => new RLDecoder(buffer, columnType)
      case DictionaryCompressionType => new DictDecoder(buffer, columnType)
      case _ => throw new UnsupportedOperationException()
    }
  }

  override def computeNext() {
    if (_decoder.hasNext) {
      _current = _decoder.next()
    }
  }

  override def hasNext = _decoder.hasNext

  override def current = _current.asInstanceOf[Object]
}

/**
 * Default representation of a Decoder. In this case the underlying buffer
 * has uncompressed data
 */
class DefaultDecoder[V](buffer: ByteBuffer, columnType: ColumnType[_, V]) extends Iterator[V] {
  private val _current: V = columnType.newWritable()

  override def hasNext = buffer.hasRemaining()

  override def next(): V = {
    columnType.extractInto(buffer, _current)
    _current
  }
}

/**
 * Run Length Decoder, decodes data compressed in RLE format of [element, length]
 */
class RLDecoder[V](buffer: ByteBuffer, columnType: ColumnType[_, V]) extends Iterator[V] {
  
  private var _run: Int = _
  private var _count: Int = 0
  private val _current: V = columnType.newWritable()

  override def hasNext = buffer.hasRemaining()
  
  override def next(): V = {
    if (_count == _run) {
      //next run
      columnType.extractInto(buffer, _current)
      _run = buffer.getInt()
      _count = 1
    } else {
      _count += 1
    }
    _current
  }
}

/**
 * Dictionary encoding compression.
 */
class DictDecoder[V](buffer: ByteBuffer, columnType: ColumnType[_, V]) extends Iterator[V] {

  // Dictionary in the form of an array. The index is the encoded value, and the value is the
  // decompressed value.
  private val _dictionary: Array[V] =  {
    val size = buffer.getInt()
    val arr = columnType.writableManifest.newArray(size)
    var count = 0
    while (count < size) {
      val writable = columnType.newWritable()
      columnType.extractInto(buffer, writable)
      arr(count) = writable.asInstanceOf[V]
      count += 1
    }
    arr
  }

  override def hasNext = buffer.hasRemaining()

  override def next(): V = {
    val index = buffer.getShort().toInt
    _dictionary(index)
  }
}
