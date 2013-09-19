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
import org.apache.hadoop.hive.serde2.`lazy`.LazyObject
import org.apache.hadoop.hive.serde2.`lazy`.LazyFactory
import org.apache.hadoop.hive.serde2.`lazy`.ByteArrayRef
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector

import shark.execution.serialization.KryoSerializer


class IntColumnIterator(buffer: ByteBuffer) extends DefaultColumnIterator(buffer, INT)

class FloatColumnIterator(buffer: ByteBuffer) extends DefaultColumnIterator(buffer, FLOAT)

class LongColumnIterator(buffer: ByteBuffer) extends DefaultColumnIterator(buffer, LONG)

class DoubleColumnIterator(buffer: ByteBuffer) extends DefaultColumnIterator(buffer, DOUBLE)

class BooleanColumnIterator(buffer: ByteBuffer) extends DefaultColumnIterator(buffer, BOOLEAN)

class ByteColumnIterator(buffer: ByteBuffer) extends DefaultColumnIterator(buffer, BYTE)

class ShortColumnIterator(buffer: ByteBuffer) extends DefaultColumnIterator(buffer, SHORT)

class NullColumnIterator(buffer: ByteBuffer) extends DefaultColumnIterator(buffer, VOID)

class TimestampColumnIterator(buffer: ByteBuffer) extends DefaultColumnIterator(buffer, TIMESTAMP)

class BinaryColumnIterator(buffer: ByteBuffer) extends DefaultColumnIterator(buffer, BINARY)

class StringColumnIterator(buffer: ByteBuffer) extends DefaultColumnIterator(buffer, STRING)

class GenericColumnIterator(buffer: ByteBuffer) extends DefaultColumnIterator(buffer, GENERIC) {

  private var _obj: LazyObject[_] = _

  override def init() {
    super.init()
    val oiSize = buffer.getInt()
    val oiSerialized = new Array[Byte](oiSize)
    buffer.get(oiSerialized, 0, oiSize)
    val oi = KryoSerializer.deserialize[ObjectInspector](oiSerialized)
    _obj = LazyFactory.createLazyObject(oi)
  }

  override def current = {
    val v = super.current.asInstanceOf[ByteArrayRef]
    _obj.init(v, 0, v.getData().length)
    _obj
  }
}

class VoidColumnIterator(buffer: ByteBuffer) extends DefaultColumnIterator(buffer, VOID)
