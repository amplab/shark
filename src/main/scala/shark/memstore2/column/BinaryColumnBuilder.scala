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

import org.apache.hadoop.hive.serde2.`lazy`.LazyBinary
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.io.BytesWritable
import org.apache.hadoop.io.Text


class BinaryColumnBuilder extends ColumnBuilder[LazyBinary] {
  // Binary column cannot be NULL in Hive.

  private var _arr: ByteArrayList = null
  private var _lengthArr: IntArrayList = null

  override def initialize(initialSize: Int) {
    _arr = new ByteArrayList(initialSize * ColumnIterator.BINARY_SIZE)
    _lengthArr = new IntArrayList(initialSize)
    super.initialize(initialSize)
  }

  override def append(o: Object, oi: ObjectInspector) {
    if (o.isInstanceOf[LazyBinary]) {
      append(o.asInstanceOf[LazyBinary])
    } else if (o.isInstanceOf[BytesWritable]) {
      append(o.asInstanceOf[BytesWritable])
    }
  }

  override def append(v: LazyBinary) {
    val w: BytesWritable = v.getWritableObject()
    append(w)
  }

  def append(w: BytesWritable) {
    _lengthArr.add(w.getLength())
    _arr.addElements(_arr.size, w.getBytes(), 0, w.getLength())
  }

  override def appendNull() {
    throw new UnsupportedOperationException("Binary column cannot be null.")
  }

  // Don't collect stats for binary types.
  override def stats: ColumnStats[LazyBinary] = null

  override def build: ByteBuffer = {
    val buf = ByteBuffer.allocate(
      _lengthArr.size * 4 + _arr.size + ColumnIterator.COLUMN_TYPE_LENGTH)
    buf.order(ByteOrder.nativeOrder())
    buf.putLong(ColumnIterator.BINARY)
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

