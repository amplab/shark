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

/** Iterator interface for a column. The iterator should be initialized by a
 * byte buffer, and next can be invoked to get the value for each cell.
 *
 * Adding a new compression/encoding scheme to the code requires several
 * things. First among them is an addition to the list of iterators here. An
 * iterator that knows how to iterate through an encoding-specific ByteBuffer is
 * required. This ByteBuffer would have been created by the one of the concrete
 * [[shark.memstore2.buffer.ColumnBuilder]] classes.  
 * 
 * 
 * The relationship/composition possibilities between the new
 * encoding/compression and existing schemes dictates how the new iterator is
 * implemented.  Null Encoding and RLE Encoding working as generic wrappers that
 * can be wrapped around any data type.  Dictionary Encoding does not work in a
 * hierarchial manner instead requiring the creation of a separate
 * DictionaryEncoded Iterator per Data type.
 * 
 * The changes required for the LZF encoding's Builder/Iterator might be the
 * easiest to look to get a feel for what is required -
 * [[shark.memstore2.buffer.LZFColumnIterator]]. See SHA 225f4d90d8721a9d9e8f
 * 
 * The base class ColumnIterator is the read side of this equation. For the
 * write side see [[shark.memstore2.buffer.ColumnBuilder]].
 * 
 */
abstract class ColumnIterator {

  def next()

  // Should be implemented as a read-only operation by the ColumnIterator
  // Can be called any number of times
  def current: Object
}

object ColumnIterator {
  
  
  def newIterator(columnType: Int, 
    buffer: ByteBuffer): ColumnIterator = {
    val v = columnType match {
      case 0 => new IntColumnIterator(buffer)
      case 1 => new LongColumnIterator(buffer)
      case 2 => new FloatColumnIterator(buffer)
      case 3 => new DoubleColumnIterator(buffer)
      case 4 => new BooleanColumnIterator(buffer)
      case 5 => new ByteColumnIterator(buffer)
      case 6 => new ShortColumnIterator(buffer)
      case 7 => new VoidColumnIterator(buffer)
      case 8 => new StringColumnIterator(buffer)
      case 9 => new TimestampColumnIterator(buffer)
      case 11 => new GenericColumnIterator(buffer)
    }
    new NullableColumnIterator(v, buffer)
  }
  
}