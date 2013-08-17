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

import org.apache.hadoop.io.IntWritable

import shark.memstore2.buffer.ByteBufferReader
import shark.memstore2.column


/** An iterator for Ints that are Dict encoded. Bytes storing the dictionary
  * come first in the buffer.
  */
object DictionaryEncodedIntColumnIterator{

  sealed class Default(private val _bytesReader: ByteBufferReader) extends ColumnIterator {
    private val _writable = new IntWritable
    private val _dict = IntDictionarySerializer.readFromBuffer(_bytesReader)

    override def next() {
      _writable.set(_dict.get(_bytesReader.getByte()))
    }

    override def current = _writable
  }
}
