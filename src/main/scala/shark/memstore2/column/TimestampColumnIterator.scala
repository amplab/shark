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

import java.sql.Timestamp

import org.apache.hadoop.hive.serde2.io.TimestampWritable

import shark.memstore2.buffer.ByteBufferReader


object TimestampColumnIterator {

  sealed class Default(private val _bytesReader: ByteBufferReader) extends ColumnIterator {
    private val _timestamp = new Timestamp(0)
    private val _writable = new TimestampWritable(_timestamp)

    override def next()  {
      _timestamp.setTime(_bytesReader.getLong())
      _timestamp.setNanos(_bytesReader.getInt())
      _writable.set(_timestamp)
    }

    override def current = _writable
  }
}
