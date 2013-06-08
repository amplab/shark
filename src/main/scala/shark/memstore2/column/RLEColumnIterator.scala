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

import shark.memstore2.buffer.ByteBufferReader
import it.unimi.dsi.fastutil.ints.IntArrayList

import shark.LogHelper

/**
  * A wrapper around any ColumnIterator so it can be Run Length Encoded
  * 
  * Builder classes should serialize the lengths of runs into the Buffer first
  * then follow that with a serialization of the actual values.
  * Run lengths are assumed to be small enough to fit into Ints.
  * 
  */

class RLEColumnIterator[T <: ColumnIterator](baseIterCls: Class[T], bytes: ByteBufferReader)
    extends ColumnIterator{
  private var lengths: IntArrayList = RLESerializer.readFromBuffer(bytes)

  private var extPos = -1
  private var currentRunPos = -1
  private var intPos = -1

  private def length = lengths.get(intPos)

  val baseIter: T = {
     val ctor = baseIterCls.getConstructor(classOf[ByteBufferReader])
     ctor.newInstance(bytes).asInstanceOf[T]
  }

  override def next() {
    // first call
    if (currentRunPos == -1) {
      intPos = 0;
      currentRunPos = 0;
      baseIter.next()
      extPos = 0
      return
    }

    extPos += 1
    // logDebug("current length " + length + " intPos " + intPos + " currentRunPos " + currentRunPos + " extPos " + extPos)

    if(currentRunPos < (length-1)) {
      // still inside run - current will return the right value
      currentRunPos += 1

    } else {
      if(currentRunPos == length-1 ) {
        currentRunPos = 0
      } else { // > 
        throw new RuntimeException("Run position longer than length of run")
      }

      intPos += 1
      baseIter.next()
      // logDebug(" pos " + extPos + " value " + current)

    }
  }

  // Semantics are to not change state - read-only
  override def current: Object = {
    if (currentRunPos == -1) {
      throw new RuntimeException("RLEColumnIterator next() should be called first")
    } else {
      baseIter.current
    }
  }
}
