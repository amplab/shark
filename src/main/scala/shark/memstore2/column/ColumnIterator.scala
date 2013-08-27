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

import java.nio.ByteBuffer
import java.nio.ByteOrder


trait ColumnIterator {

  private var _initialized = false
  
  def init() {}

  def next() {
    if (!_initialized) {
      init()
      _initialized = true
    }
    computeNext()
  }

  def computeNext(): Unit

  // Should be implemented as a read-only operation by the ColumnIterator
  // Can be called any number of times
  def current: Object
}


abstract class DefaultColumnIterator[T, V](val buffer: ByteBuffer, val columnType: ColumnType[T, V])
  extends CompressedColumnIterator


object Implicits {
  implicit def intToCompressionType(i: Int): CompressionType = i match {
    case -1 => DefaultCompressionType
    case 0 => RLECompressionType
    case 1 => DictionaryCompressionType
    case _ => throw new UnsupportedOperationException("Compression Type " + i)
  }

  implicit def intToColumnType(i: Int): ColumnType[_, _] = i match {
    case 0 => INT
    case 1 => LONG
    case 2 => FLOAT
    case 3 => DOUBLE
    case 4 => BOOLEAN
    case 5 => BYTE
    case 6 => SHORT
    case 7 => VOID
    case 8 => STRING
    case 9 => TIMESTAMP
    case 10 => BINARY
    case 11 => GENERIC
  }
}

object ColumnIterator {

  import shark.memstore2.column.Implicits._

  def newIterator(b: ByteBuffer): ColumnIterator = {
    val buffer = b.duplicate().order(ByteOrder.nativeOrder())
    val columnType: ColumnType[_, _] = buffer.getInt()
    val v = columnType match {
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
    new NullableColumnIterator(v, buffer)
  }
}
