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

import javaewah.EWAHCompressedBitmap
import javaewah.EWAHCompressedBitmapSerializer
import javaewah.IntIterator

import shark.memstore2.buffer.ByteBufferReader


/** Wrapper around Concrete ColumnIterators so they can handle null values.
  * The Null Bit Vector is maintained in the first part of the Buffer.
  * Can be composed with other wrappers like the [[shark.memstore2.column.RLEColumnIterator]]
  */
class EWAHNullableColumnIterator[T <: ColumnIterator](bIter: T, bytes: ByteBufferReader)
  extends ColumnIterator {

  val _nullBitmap: EWAHCompressedBitmap = EWAHCompressedBitmapSerializer.readFromBuffer(bytes)
  // The location of the null bits is returned, in increasing order through
  // _nullsIter
  val _nullsIter: IntIterator = _nullBitmap.intIterator

  var _pos = -1
  var _nextNullPosition = -1
  private var currentNull = false

  // Construction using a reference instead of classname - used to compose with
  // other encodings like RLE
  var baseIter: T = bIter

  /** auxiliary constructor for most construction */
  def this(baseIterCls: Class[T], bytes: ByteBufferReader) = {
    this( { 
      val ctorThrowAway = baseIterCls.getConstructor(classOf[ByteBufferReader])
      // use .duplicate() so that this can be thrown away
      ctorThrowAway.newInstance(bytes.duplicate).asInstanceOf[T] // not used
    },
      bytes)
    // overwrite construction and get new one from the right position in the ByteBufferReader
    val ctor = baseIterCls.getConstructor(classOf[ByteBufferReader])
    baseIter = ctor.newInstance(bytes).asInstanceOf[T]
  }

  override def next() {
    if (_pos >= _nextNullPosition || _pos == -1) {
      if (_nullsIter.hasNext) {
        _nextNullPosition = _nullsIter.next
      }
    }

    _pos += 1
    // println(" _nextNullPosition " + _nextNullPosition + " ][ _pos " + _pos)

    if (_pos == _nextNullPosition) {
      null
    } else {
      baseIter.next()
    }
  }

  // Semantics are to not change state - read-only
  override def current: Object = {
    if (_nextNullPosition == _pos) {
      null
    } else {
      baseIter.current
    }
  }
}
