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

import org.apache.hadoop.hive.serde2.`lazy`.{ByteArrayRef, LazyFactory, LazyObject}
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.io.Text

import shark.execution.serialization.KryoSerializer
import shark.memstore2.buffer.ByteBufferReader


object ComplexColumnIterator {

  sealed class Default(private val _bytesReader: ByteBufferReader) extends ColumnIterator {

    private var _obj: LazyObject[_] = {
      val oiSize = _bytesReader.getLong()
      val oiSerialized = new Array[Byte](oiSize.toInt)
      _bytesReader.getBytes(oiSerialized, oiSize.toInt)
      val oi = KryoSerializer.deserialize[ObjectInspector](oiSerialized)
      LazyFactory.createLazyObject(oi)
    }

    private var _byteArrayRef = new ByteArrayRefWithReader(_bytesReader)

    override def next() = {
      val len = _bytesReader.getInt
      _byteArrayRef.readFromReader(len)
      _obj.init(_byteArrayRef, 0, len)
    }

    override def current = _obj

    // A custom ByteArrayRef implementation that reads directly from a ByteBufferReader.
    class ByteArrayRefWithReader(reader: ByteBufferReader) extends ByteArrayRef {

      private var _data: Array[Byte] = new Array[Byte](32)

      override def getData(): Array[Byte] = _data

      override def setData(data: Array[Byte]) {
        _data = data
      }

      def readFromReader(size: Int) {
        if (_data.size < size) {
          _data = new Array[Byte](size)
        }
        reader.getBytes(_data, size)
      }
    }
  }
}
