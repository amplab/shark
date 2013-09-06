package shark.memstore2.column

import java.nio.{ByteBuffer, ByteOrder}

import scala.annotation.tailrec
import scala.collection.mutable.HashMap

/**
 * API for Compression
 */
trait CompressionAlgorithm  {

  def compressionType: CompressionType

  /**
   * Tests whether the compression algorithm supports a specific column type.
   */
  def supportsType(t: ColumnType[_, _]): Boolean

  /**
   * Collect a value so we can update the compression ratio for this compression algorithm.
   */
  def gatherStatsForCompressibility[T](v: T, t: ColumnType[T, _])

  /**
   * Return compression ratio between 0 and 1, smaller score imply higher compressibility.
   */
  def compressionRatio: Double

  /**
   * Compress the given buffer and return the compressed data as a new buffer.
   */
  def compress[T](b: ByteBuffer, t: ColumnType[T, _]): ByteBuffer
}


case class CompressionType(typeID: Int)

object DefaultCompressionType extends CompressionType(-1)

object RLECompressionType extends CompressionType(0)

object DictionaryCompressionType extends CompressionType(1)


class NoCompression extends CompressionAlgorithm {
  override def compressionType = DefaultCompressionType

  override def supportsType(t: ColumnType[_,_]) = true

  override def gatherStatsForCompressibility[T](v: T, t: ColumnType[T,_]) {}

  override def compressionRatio: Double = 1.0

  override def compress[T](b: ByteBuffer, t: ColumnType[T, _]) = {
    val len = b.limit()
    val newBuffer = ByteBuffer.allocate(len + 4)
    newBuffer.order(ByteOrder.nativeOrder())
    newBuffer.putInt(b.getInt())
    newBuffer.putInt(compressionType.typeID)
    newBuffer.put(b)
    b.clear()
    newBuffer.rewind()
    newBuffer
  }
}

/**
 * Run-length encoding for columns with a lot of repeated values.
 */
class RLE extends CompressionAlgorithm {
  private var _uncompressedSize: Int = 0
  private var _compressedSize: Int = 0

  // Previous element, used to track how many runs and the run lengths.
  private var _prev: Any = _
  // Current run length.
  private var _run: Int = 0

  override def compressionType = RLECompressionType

  override def supportsType(t: ColumnType[_, _]) = {
    t match {
      case LONG | INT | STRING | SHORT | BYTE | BOOLEAN => true
      case _ => false
    }
  }

  override def gatherStatsForCompressibility[T](v: T, t: ColumnType[T,_]) {
    val s = t.actualSize(v)
    if (_prev == null) {
      // This is the very first run.
      _prev = t.clone(v)
      _run = 1
    } else {
      if (_prev.equals(v)) {
        // Add one to the current run's length.
        _run += 1
      } else {
        // Start a new run. Update the current run length.
        _compressedSize += (t.actualSize(_prev.asInstanceOf[T]) + 4)
        _prev = t.clone(v)
        _run = 1
      }
    }
    _uncompressedSize += s
  }

  // Note that we don't actually track the size of the last run into account to simplify the
  // logic a little bit.
  override def compressionRatio = _compressedSize / (_uncompressedSize + 0.0)

  override def compress[T](b: ByteBuffer, t: ColumnType[T,_]): ByteBuffer = {
    // Add the size of the last run to the _size
    if (_prev != null) {
      _compressedSize += t.actualSize(_prev.asInstanceOf[T]) + 4
    }

    val compressedBuffer = ByteBuffer.allocate(_compressedSize + 4 + 4)
    compressedBuffer.order(ByteOrder.nativeOrder())
    compressedBuffer.putInt(b.getInt())
    compressedBuffer.putInt(compressionType.typeID)
    encode(b, compressedBuffer, null, t)
    compressedBuffer.rewind()
    compressedBuffer
  }

  @tailrec private final def encode[T](currentBuffer: ByteBuffer,
      compressedBuffer: ByteBuffer, currentRun: (T, Int), t: ColumnType[T,_]) {
    def writeOutRun() {
      t.append(currentRun._1, compressedBuffer)
      compressedBuffer.putInt(currentRun._2)
    }
    if (!currentBuffer.hasRemaining()) {
      writeOutRun()
      return
    }
    val elem = t.extract(currentBuffer.position(), currentBuffer)
    val newRun =
      if (currentRun == null) {
        (elem, 1)
      } else if (currentRun._1.equals(elem)) {
        //update length
        (currentRun._1, currentRun._2 + 1)
      } else {
        //write out the current run to compressed buffer
        writeOutRun()
        (elem, 1)
      }
    encode(currentBuffer, compressedBuffer, newRun, t)
  }
}

/**
 * Dictionary encoding for columns with small cardinality.
 */
class DictionaryEncoding extends CompressionAlgorithm {

  private val MAX_DICT_SIZE = Short.MaxValue - 1 // 32K unique values allowed
  private var _dictionary = new HashMap[Any, Short]()

  // We use a short integer to store the dictionary index, which takes 2 bytes.
  private val indexSize = 2

  // Size of the dictionary, in bytes. Initialize the dictionary size to 4 since we use an int
  // to store the number of elements in the dictionary.
  private var _dictionarySize = 4

  // Size of the input, uncompressed, in bytes. Note that we only count until the dictionary
  // overflows.
  private var _uncompressedSize = 0

  // Total number of elements.
  private var _count = 0

  // If the number of distinct elements is too large, we discard the use of dictionary
  // encoding and set the overflow flag to true.
  private var _overflow = false

  override def compressionType = DictionaryCompressionType

  override def supportsType(t: ColumnType[_, _]) = t match {
    case STRING | LONG | INT => true
    case _ => false
  }

  override def gatherStatsForCompressibility[T](v: T, t: ColumnType[T, _]) {
    // Use this function to build up a dictionary.
    if (!_overflow) {
      val size = t.actualSize(v)
      _count += 1
      _uncompressedSize += size

      if (!_dictionary.contains(v)) {
        // The dictionary doesn't contain the value. Add the value to the dictionary if we haven't
        // overflown yet.
        if (_dictionary.size < MAX_DICT_SIZE) {
          _dictionary.put(t.clone(v), _dictionary.size.toShort)
          _dictionarySize += size + indexSize
        } else {
          // Overflown. Release the dictionary immediately to lower memory pressure.
          _overflow = true
          _dictionary = null
        }
      }
    }
  }

  /**
   * Return the compression ratio if encoded with dictionary encoding. If the dictionary
   * cardinality (i.e. the number of distinct elements) is bigger than 32K, we return an
   * arbitrary number greater than 1.0.
   */
  override def compressionRatio: Double = compressedSize / (_uncompressedSize + 0.0)

  private def compressedSize: Int = {
    // Total compressed size =
    //   size of the dictionary +
    //   the number of elements * dictionary encoded size (short) +
    //   an integer for compression type
    //   an integer for column type
    if (_overflow) Int.MaxValue else _dictionarySize + _count * indexSize + 4 + 4
  }

  override def compress[T](b: ByteBuffer, t: ColumnType[T, _]): ByteBuffer = {
    if (_overflow) {
      throw new MemoryStoreException(
        "Dictionary encoding should not be used because we have overflown the dictionary.")
    }

    // Create a new buffer and store the compression type and column type.
    val compressedBuffer = ByteBuffer.allocate(compressedSize)
    compressedBuffer.order(ByteOrder.nativeOrder())
    compressedBuffer.putInt(b.getInt())
    compressedBuffer.putInt(compressionType.typeID)

    // Write out the dictionary.
    compressedBuffer.putInt(_dictionary.size)
    _dictionary.foreach { x =>
      t.append(x._1.asInstanceOf[T], compressedBuffer)
      compressedBuffer.putShort(x._2)
    }

    // Write out the encoded values, each is represented by a short integer.
    while (b.hasRemaining()) {
      val v = t.extract(b.position(), b)
      compressedBuffer.putShort(_dictionary(v))
    }

    // Rewind the compressed buffer and return it.
    compressedBuffer.rewind()
    compressedBuffer
  }
}
