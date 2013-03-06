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

import org.apache.hadoop.io.BytesWritable


class BinaryColumnIterator extends ColumnIterator {

  private val _writable = new BytesWritable

  override def next: Object = {
    // Ideally we want to set the bytes in BytesWritable directly without creating
    // a new byte array.
    val length = _bytesReader.getInt
    if (length >= 0) {
      val newBytes = new Array[Byte](length)
      _bytesReader.getBytes(newBytes, length)
      _writable.set(newBytes, 0, newBytes.size)
      _writable
    } else {
      null
    }
  }

  override def current = _writable
}