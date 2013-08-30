package shark.memstore2.column

import java.nio.ByteBuffer
import java.nio.ByteOrder
import scala.annotation.tailrec
import scala.collection.mutable.HashMap

/**
 * API for Compression
 */
trait CompressionAlgorithm  {

  def compressionType: CompressionType

  def supportsType(t: ColumnType[_, _]): Boolean

  def gatherStatsForCompressibility[T](v: T, t: ColumnType[T, _])

  /**
   * Return compression ratio between 0 and 1, smaller score imply higher compressibility.
   */
  def compressionRatio: Double

  def compress[T](b: ByteBuffer, t: ColumnType[T, _]): ByteBuffer
}


case class CompressionType(typeID: Int)

object DefaultCompressionType extends CompressionType(-1)

object RLECompressionType extends CompressionType(0)
object DictionaryCompressionType extends CompressionType(1)
object RLEVariantCompressionType extends CompressionType(2)


class NoCompression extends CompressionAlgorithm {
  override def compressionType = DefaultCompressionType

  override def supportsType(t: ColumnType[_,_]) = true

  override def gatherStatsForCompressibility[T](v: T, t: ColumnType[T,_]) = {}

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
 * Implements Run Length Encoding
 */
class RLE extends CompressionAlgorithm {
  private var _total: Int = 0
  private var _prev: Any = _
  private var _run: Int = 0
  private var _size: Int = 0

  override def compressionType = RLECompressionType

  override def supportsType(t: ColumnType[_, _]) = {
    t match {
      case INT | STRING | SHORT | BYTE | BOOLEAN => true
      case _ => false
    }
  }

  override def gatherStatsForCompressibility[T](v: T, t: ColumnType[T,_]) = {
    val s = t.actualSize(v)
    if (_prev == null) {
      _prev = t.clone(v)
      _run = 1
    } else {
      if (_prev.equals(v)) {
        _run += 1
      } else {
        // flush run into size
        _size += (t.actualSize(_prev.asInstanceOf[T]) + 4)
        _prev = t.clone(v)
        _run = 1
      }
    }
    _total += s
  }

  // Note that we don't actually track the size of the last run into account to simplify the
  // logic a little bit.
  override def compressionRatio = _size / (_total + 0.0)

  override def compress[T](b: ByteBuffer, t: ColumnType[T,_]) = {
    // Add the size of the last run to the _size
    if (_prev != null) {
      _size += t.actualSize(_prev.asInstanceOf[T]) + 4
    }

    val compressedBuffer = ByteBuffer.allocate(_size + 4 + 4)
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

class DictionaryEncoding extends CompressionAlgorithm {

  private val MAX_DICT_SIZE = 4000
  private val _dictionary = new HashMap[Any, Int]()
  private var _dictionarySize = 0
  private var _totalSize = 0
  private var _count = 0
  private var _index = 0
  private var _overflow = false

  override def compressionType = DictionaryCompressionType

  override def supportsType(t: ColumnType[_, _]) = t match {
    case STRING => true
    case _ => false
  }

  private def encode[T](v: T, t: ColumnType[T, _], sizeFunc:T => Int): Int = {
    _count += 1
    val size = sizeFunc(v)
    _totalSize += size
    if (_dictionary.size < MAX_DICT_SIZE) {
      val s = t.clone(v)
      _dictionary.get(s) match {
        case Some(index) => index
        case None => {
          _dictionary.put(s, _index)
          _index += 1
          _dictionarySize += (size + 4)
          _index
        }
      }
    } else {
      _overflow = true
      -1
    }
  }

  override def gatherStatsForCompressibility[T](v: T, t: ColumnType[T, _]) = {
    //need an estimate of the # of uniques so we can build an appropriate
    //dictionary if needed. More precisely, we only need a lower bound
    //on # of uniques.
    val size = t.actualSize(v)
    encode(v, t, { _:T => size})
  }

  /**
   * return score between 0 and 1, smaller score imply higher compressibility.
   */
  override def compressionRatio: Double = {
    if (_overflow) 1.0 else (_count*4 + dictionarySize) / (_totalSize + 0.0)
  }

  private def writeDictionary[T](compressedBuffer: ByteBuffer, t: ColumnType[T, _]) {
    //store dictionary size
    compressedBuffer.putInt(_dictionary.size)
    //store the dictionary
    _dictionary.foreach { x =>
      t.append(x._1.asInstanceOf[T], compressedBuffer)
      compressedBuffer.putInt(x._2)
    }
  }

  private def dictionarySize = _dictionarySize + 4

  override def compress[T](b: ByteBuffer, t: ColumnType[T, _]): ByteBuffer = {
    //build a dictionary of given size
    val compressedBuffer = ByteBuffer.allocate(_count*4 + dictionarySize + 4 + 4)
    compressedBuffer.order(ByteOrder.nativeOrder())
    compressedBuffer.putInt(b.getInt())
    compressedBuffer.putInt(compressionType.typeID)
    //store dictionary size
    writeDictionary(compressedBuffer, t)
    //traverse the original buffer
    while (b.hasRemaining()) {
      val v = t.extract(b.position(), b)
      _dictionary.get(v).map { index =>
        compressedBuffer.putInt(index)
      }
      
    }
    compressedBuffer.rewind()
    compressedBuffer
  }
}