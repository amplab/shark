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

import it.unimi.dsi.fastutil.bytes.ByteArrayList
import it.unimi.dsi.fastutil.ints.IntArrayList

import org.apache.hadoop.hive.serde2.ByteStream
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector

import shark.execution.serialization.KryoSerializer


class ComplexColumnBuilder(oi: ObjectInspector) extends ColumnBuilder[ByteStream.Output] {

  private var _arr: ByteArrayList = null
  private var _lengthArr: IntArrayList = null

  override def initialize(initialSize: Int) {
    _arr = new ByteArrayList(initialSize * ColumnIterator.COMPLEX_TYPE_SIZE)
    _lengthArr = new IntArrayList(initialSize)
    super.initialize(initialSize)
  }

  override def append(o: Object, oi: ObjectInspector) {
    append(o.asInstanceOf[ByteStream.Output])
  }

  override def appendNull() {
    // A complex data type is always serialized before passing into this. It is not
    // possible to be null.
    throw new UnsupportedOperationException
  }

  override def append(v: ByteStream.Output) {
    _lengthArr.add(v.getCount)
    _arr.addElements(_arr.size(), v.getData, 0, v.getCount)
  }

  // Don't collect stats for complex data types.
  override def stats = null

  override def build: ByteBuffer = {
    val buf = ByteBuffer.allocate(
      _lengthArr.size * 4 + _arr.size + ColumnIterator.COLUMN_TYPE_LENGTH)
    buf.order(ByteOrder.nativeOrder())
    buf.putLong(ColumnIterator.COMPLEX)

    val objectInspectorSerialized = KryoSerializer.serialize(oi)
    buf.putLong(objectInspectorSerialized.size)
    buf.put(objectInspectorSerialized)

    var i = 0
    var runningOffset = 0
    while (i < _lengthArr.size) {
      val len = _lengthArr.get(i)
      buf.putInt(len)
      buf.put(_arr.elements(), runningOffset, len)
      runningOffset += len
      i += 1
    }
    buf.rewind()
    buf
  }
}
