/*
 * Copyright (C) 2012 The Regents of The University California.
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package shark.memstore2.column

import java.nio.{ByteBuffer, ByteOrder}

import scala.annotation.tailrec
import scala.collection.mutable.{ArrayBuffer, HashMap}

/**
 * API for Compression
 */
trait CompressionAlgorithm {

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
   * Return compression ratio, smaller scores imply higher compressibility.
   * Scores greater than 1.0 indicate that compressed data will take up more space than uncompressed
   * data. This number is used to pick the compression algorithm to apply at runtime.
   */
  def compressionRatio: Double = compressedSize.toDouble / uncompressedSize.toDouble

  /**
   * The uncompressed size of the input data.
   */
  def uncompressedSize: Int

  /**
   * Estimation of the data size once compressed.
   */
  def compressedSize: Int

  /**
   * Compress the given buffer and return the compressed data as a new buffer.
   */
  def compress[T](b: ByteBuffer, t: ColumnType[T, _]): ByteBuffer
}

case class CompressionType(typeID: Int)

object DefaultCompressionType extends CompressionType(-1)

object RLECompressionType extends CompressionType(0)

object DictionaryCompressionType extends CompressionType(1)

object BooleanBitSetCompressionType extends CompressionType(2)

object IntDeltaCompressionType extends CompressionType(3)

object LongDeltaCompressionType extends CompressionType(4)

/**
 * An no-op compression.
 */
class NoCompression extends CompressionAlgorithm {

  override def compressionType = DefaultCompressionType

  override def supportsType(t: ColumnType[_, _]) = true

  override def gatherStatsForCompressibility[T](v: T, t: ColumnType[T, _]) {}

  override def compressionRatio: Double = 1.0

  override def uncompressedSize: Int = 0

  override def compressedSize: Int = 0

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
 * Run-length encoding for columns with a lot of repeated values that occur next to each other.
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

  override def gatherStatsForCompressibility[T](v: T, t: ColumnType[T, _]) {
    val s = t.actualSize(v)
    if (_prev == null) {
      // This is the very first run.
      _prev = t.clone(v)
      _run = 1
      _compressedSize += s + 4
    } else {
      if (_prev.equals(v)) {
        // Add one to the current run's length.
        _run += 1
      } else {
        // Start a new run. Update the current run length.
        _compressedSize += s + 4
        _prev = t.clone(v)
        _run = 1
      }
    }
    _uncompressedSize += s
  }

  override def uncompressedSize: Int = _uncompressedSize

  // Note that we don't actually track the size of the last run into account to simplify the
  // logic a little bit.
  override def compressedSize: Int = _compressedSize

  override def compress[T](b: ByteBuffer, t: ColumnType[T, _]): ByteBuffer = {
    // Leave 4 extra bytes for column type and another 4 for compression type.
    val compressedBuffer = ByteBuffer.allocate(4 + 4 + _compressedSize)
    compressedBuffer.order(ByteOrder.nativeOrder())
    compressedBuffer.putInt(b.getInt())
    compressedBuffer.putInt(compressionType.typeID)
    encode(b, compressedBuffer, null, t)
    compressedBuffer.rewind()
    compressedBuffer
  }

  @tailrec private final def encode[T](currentBuffer: ByteBuffer,
      compressedBuffer: ByteBuffer, currentRun: (T, Int), t: ColumnType[T, _]) {
    def writeOutRun() {
      t.append(currentRun._1, compressedBuffer)
      compressedBuffer.putInt(currentRun._2)
    }
    if (!currentBuffer.hasRemaining()) {
      writeOutRun()
      return
    }
    val elem = t.extract(currentBuffer)
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
 * Dictionary encoding for columns with small cardinality. This algorithm encodes values into
 * short integers (2 byte each). It can support up to 32k distinct values.
 */
class DictionaryEncoding extends CompressionAlgorithm {

  // 32K unique values allowed
  private val MAX_DICT_SIZE = Short.MaxValue - 1

  // The dictionary that maps a value to the encoded short integer.
  private var _dictionary = new HashMap[Any, Short]()

  // The reverse mapping of _dictionary, i.e. mapping encoded integer to the value itself.
  private var _values = new ArrayBuffer[Any](1024)

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

  override def supportsType(t: ColumnType[_, _]) = {
    t match {
      case STRING | LONG | INT => true
      case _ => false
    }
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
          val clone = t.clone(v)
          _values.append(clone)
          _dictionary.put(clone, _dictionary.size.toShort)
          _dictionarySize += size
        } else {
          // Overflown. Release the dictionary immediately to lower memory pressure.
          _overflow = true
          _values = null
          _dictionary = null
        }
      }
    }
  }

  override def uncompressedSize: Int = _uncompressedSize

  /**
   * Return the compressed data size if encoded with dictionary encoding. If the dictionary
   * cardinality (i.e. the number of distinct elements) is bigger than 32K, we return an
   * a really large number so that dictionary encoding does not get used on this column.
   */
  override def compressedSize: Int = {
    // Total compressed size =
    //   size of the dictionary +
    //   the number of elements * dictionary encoded size (short)
    if (_overflow) Int.MaxValue else _dictionarySize + _count * indexSize
  }

  override def compress[T](b: ByteBuffer, t: ColumnType[T, _]): ByteBuffer = {
    if (_overflow) {
      throw new MemoryStoreException(
        "Dictionary encoding should not be used because we have overflowed the dictionary.")
    }

    // Create a new buffer and store the compression type(4) and column type(4)
    val compressedBuffer = ByteBuffer.allocate(4 + 4 + compressedSize)
    compressedBuffer.order(ByteOrder.nativeOrder())
    compressedBuffer.putInt(b.getInt())
    compressedBuffer.putInt(compressionType.typeID)

    // Write out the dictionary.
    compressedBuffer.putInt(_dictionary.size)
    _values.foreach { v =>
      t.append(v.asInstanceOf[T], compressedBuffer)
    }

    // Write out the encoded values, each is represented by a short integer.
    while (b.hasRemaining()) {
      val v = t.extract(b)
      compressedBuffer.putShort(_dictionary(v))
    }

    // Rewind the compressed buffer and return it.
    compressedBuffer.rewind()
    compressedBuffer
  }
}

/**
 * BitSet compression for Boolean values.
 */
object BooleanBitSetCompression {
  val BOOLEANS_PER_LONG: Short = 64
}

class BooleanBitSetCompression extends CompressionAlgorithm {

  private var _uncompressedSize = 0

  override def compressionType = BooleanBitSetCompressionType

  override def supportsType(t: ColumnType[_, _]) = {
    t match {
      case BOOLEAN => true
      case _ => false
    }
  }

  override def gatherStatsForCompressibility[T](v: T, t: ColumnType[T, _]) {
    val s = t.actualSize(v)
    _uncompressedSize += s
  }

  // Booleans are encoded into Longs; in addition, we need one int to store the number of
  // Booleans contained in the compressed buffer.
  override def compressedSize: Int = {
    math.ceil(_uncompressedSize.toFloat / BooleanBitSetCompression.BOOLEANS_PER_LONG).toInt * 8 + 4
  }

  override def uncompressedSize: Int = _uncompressedSize

  override def compress[T](b: ByteBuffer, t: ColumnType[T, _]): ByteBuffer = {
    // Leave 4 extra bytes for column type, another 4 for compression type - header
    val compressedBuffer = ByteBuffer.allocate(4 + 4 + compressedSize)
    compressedBuffer.order(ByteOrder.nativeOrder())
    compressedBuffer.putInt(b.getInt())
    compressedBuffer.putInt(compressionType.typeID)
    compressedBuffer.putInt(b.remaining())

    var cur: Long = 0
    var pos: Int = 0
    var offset: Int = 0

    while (b.hasRemaining) {
      offset = pos % BooleanBitSetCompression.BOOLEANS_PER_LONG
      val elem = t.extract(b).asInstanceOf[Boolean]

      if (elem) {
        cur = (cur | (1 << offset)).toLong
      }
      if (offset == BooleanBitSetCompression.BOOLEANS_PER_LONG - 1 || !b.hasRemaining) {
        compressedBuffer.putLong(cur)
        cur = 0
      }
      pos += 1
    }
    // Rewind the compressed buffer and return it.
    compressedBuffer.rewind()
    compressedBuffer
  }
}

/**
 * Delta encoding for 32-bit integers. The integers are encoded as follows:
 *
 * The first byte is a delta byte. If the value of the delta byte is -128 (i.e. Byte.MinValue),
 * we need to read the next 4 full bytes to get a new base value. If the value of the delta byte
 * is anything else (i.e. from -127 inclusively to 127 inclusively), then its a delta value.
 */
class IntDeltaEncoding extends CompressionAlgorithm {

  private var prev = 0
  private var first = true

  override def compressionType = IntDeltaCompressionType

  override def supportsType(t: ColumnType[_, _]) = t == INT

  var _compressedSize: Int = 0
  var _uncompressedSize: Int = 0

  override def uncompressedSize: Int = _uncompressedSize
  override def compressedSize: Int = _compressedSize

  override def gatherStatsForCompressibility[T](v: T, t: ColumnType[T, _]) {
    val currentValue = v.asInstanceOf[Int]
    if (first) {
      first = false
      _compressedSize += 1 + INT.actualSize(currentValue) // always 1 + 4
      prev = currentValue
    } else {
      val delta = currentValue - prev
      if (math.abs(delta) > 127) {
        _compressedSize += 1 + INT.actualSize(currentValue) // 1 + 4
      } else {
        _compressedSize += 1
      }
      prev = currentValue
    }
    _uncompressedSize += INT.actualSize(currentValue)
  }

  override def compress[V](b: ByteBuffer, t: ColumnType[V, _]): ByteBuffer = {
    first = true

    // Leave 4 extra bytes for column type, another 4 for compression type - header
    val compressedBuffer = ByteBuffer.allocate(4 + 4 + compressedSize)
    compressedBuffer.order(ByteOrder.nativeOrder())
    compressedBuffer.putInt(b.getInt())
    compressedBuffer.putInt(compressionType.typeID)

    while (b.hasRemaining) {
      val current = INT.extract(b)
      val delta = current - prev
      prev = current

      if (math.abs(delta) > 127 || first) {
        first = false
        compressedBuffer.put(Byte.MinValue)
        INT.append(current, compressedBuffer)
      } else {
        compressedBuffer.put(delta.toByte)
      }
    }

    compressedBuffer.rewind()
    compressedBuffer
  }
}

/**
 * Delta encoding for 64-bit integers. The integers are encoded as follows:
 *
 * The first byte is a delta byte. If the value of the delta byte is -128 (i.e. Byte.MinValue),
 * we need to read the next 8 full bytes to get a new base value. If the value of the delta byte
 * is anything else (i.e. from -127 inclusively to 127 inclusively), then its a delta value.
 */
class LongDeltaEncoding extends CompressionAlgorithm {

  private var prev = 0L
  private var first = true

  override def compressionType = LongDeltaCompressionType

  override def supportsType(t: ColumnType[_, _]) = t == LONG

  var _compressedSize: Int = 0
  var _uncompressedSize: Int = 0

  override def uncompressedSize: Int = _uncompressedSize
  override def compressedSize: Int = _compressedSize

  override def gatherStatsForCompressibility[T](v: T, t: ColumnType[T, _]) {
    val currentValue = v.asInstanceOf[Long]
    if (first) {
      first = false
      _compressedSize += 1 + LONG.actualSize(currentValue) // always 1 + 8
      prev = currentValue
    } else {
      val delta = currentValue - prev
      if (math.abs(delta) > 127) {
        _compressedSize += 1 + LONG.actualSize(currentValue) // 1 + 8
      } else {
        _compressedSize += 1
      }
      prev = v.asInstanceOf[Long]
    }
    _uncompressedSize += LONG.actualSize(currentValue)
  }

  override def compress[V](b: ByteBuffer, t: ColumnType[V, _]): ByteBuffer = {
    first = true

    // Leave 4 extra bytes for column type, another 4 for compression type - header
    val compressedBuffer = ByteBuffer.allocate(4 + 4 + compressedSize)
    compressedBuffer.order(ByteOrder.nativeOrder())
    compressedBuffer.putInt(b.getInt())
    compressedBuffer.putInt(compressionType.typeID)

    while (b.hasRemaining) {
      val current = LONG.extract(b)
      val delta = current - prev
      prev = current

      if (math.abs(delta) > 127 || first) {
        first = false
        compressedBuffer.put(Byte.MinValue)
        LONG.append(current, compressedBuffer)
      } else {
        compressedBuffer.put(delta.toByte)
      }
    }

    compressedBuffer.rewind()
    compressedBuffer
  }
}
