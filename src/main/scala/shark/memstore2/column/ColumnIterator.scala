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

import scala.language.implicitConversions

import java.nio.{ByteBuffer, ByteOrder}

trait ColumnIterator {

  init()

  def init() {}

  /**
   * Produces the next element of this iterator.
   */
  def next()

  /**
   * Tests whether this iterator can provide another element.
   */
  def hasNext: Boolean

  /**
   * Return the current element. The operation should have no side-effect, i.e. it can be invoked
   * multiple times returning the same value.
   */
  def current: Object
}

abstract class DefaultColumnIterator[T, V](val buffer: ByteBuffer, val columnType: ColumnType[T, V])
  extends CompressedColumnIterator{}

object Implicits {
  implicit def intToCompressionType(i: Int): CompressionType = i match {
    case DefaultCompressionType.typeID => DefaultCompressionType
    case RLECompressionType.typeID => RLECompressionType
    case DictionaryCompressionType.typeID => DictionaryCompressionType
    case BooleanBitSetCompressionType.typeID => BooleanBitSetCompressionType
    case IntDeltaCompressionType.typeID => IntDeltaCompressionType
    case LongDeltaCompressionType.typeID => LongDeltaCompressionType
    case _ => throw new MemoryStoreException("Unknown compression type " + i)
  }

  implicit def compressionTypeToString(c: CompressionType): String = c match {
    case DefaultCompressionType => "Default"
    case RLECompressionType => "RLE"
    case DictionaryCompressionType => "Dictionary"
    case BooleanBitSetCompressionType => "BooleanBitSet"
    case IntDeltaCompressionType => "IntDelta"
    case LongDeltaCompressionType => "LongDelta"
    case _ => throw new MemoryStoreException("Unknown compression type " + c.typeID)
  }

  implicit def intToColumnType(i: Int): ColumnType[_, _] = i match {
    case INT.typeID => INT
    case LONG.typeID => LONG
    case FLOAT.typeID => FLOAT
    case DOUBLE.typeID => DOUBLE
    case BOOLEAN.typeID => BOOLEAN
    case BYTE.typeID => BYTE
    case SHORT.typeID => SHORT
    case VOID.typeID => VOID
    case STRING.typeID => STRING
    case TIMESTAMP.typeID => TIMESTAMP
    case BINARY.typeID => BINARY
    case GENERIC.typeID => GENERIC
    case _ => throw new MemoryStoreException("Unknown column type " + i)
  }
}

object ColumnIterator {

  import shark.memstore2.column.Implicits._

  def newIterator(b: ByteBuffer): ColumnIterator = {
    new NullableColumnIterator(b.duplicate().order(ByteOrder.nativeOrder()))
  }

  def newNonNullIterator(b: ByteBuffer): ColumnIterator = {
    // The first 4 bytes in the buffer indicates the column type.
    val buffer = b.duplicate().order(ByteOrder.nativeOrder())
    val columnType: ColumnType[_, _] = buffer.getInt()
    columnType match {
      case INT => new IntColumnIterator(buffer)
      case LONG => new LongColumnIterator(buffer)
      case FLOAT => new FloatColumnIterator(buffer)
      case DOUBLE => new DoubleColumnIterator(buffer)
      case BOOLEAN => new BooleanColumnIterator(buffer)
      case BYTE => new ByteColumnIterator(buffer)
      case SHORT => new ShortColumnIterator(buffer)
      case VOID => new VoidColumnIterator(buffer)
      case STRING => new StringColumnIterator(buffer)
      case BINARY => new BinaryColumnIterator(buffer)
      case TIMESTAMP => new TimestampColumnIterator(buffer)
      case GENERIC => new GenericColumnIterator(buffer)
    }
  }
}
