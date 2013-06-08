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
import org.apache.hadoop.io


import shark.LogHelper

/**
  * A wrapper around any ColumnIterator so it can be Dictionary Encoded
  * 
  *  Bytes storing the dictionary come first in the buffer.
  * 
  */

class DictionaryEncodedColumnIterator[Iter <: ColumnIterator](baseIterCls: Class[Iter], _bytesReader: ByteBufferReader)
    extends ColumnIterator{
  private var currentByte: Byte = 0
  private var initialized =  false
  private val _dict = DictionarySerializer.readFromBuffer(_bytesReader)
  // order matters - dict should be built first
  // private val baseIter: Iter = {
  //   val ctor = baseIterCls.getConstructor(classOf[ByteBufferReader])
  //   ctor.newInstance(_bytesReader).asInstanceOf[Iter]
  // }


  override def next() {
    initialized = true
    currentByte = _bytesReader.getByte()
  }

  // Semantics are to not change state - read-only
  override def current: Object = {
    if (!initialized) {
      // require(initialized == true)
      throw new RuntimeException("RLEColumnIterator next() should be called first")
    } else {
      _dict.getWritable(currentByte)
    }
  }

}
