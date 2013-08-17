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

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector
import org.apache.hadoop.io.Text


class StringColumnBuilder extends ColumnBuilder[Text] {

  private var _stats: ColumnStats.StringColumnStats = null

  // In string, a length of -1 is used to represent null values.
  private val NULL_VALUE = -1
  private var _arr: ByteArrayList = null
  private var _lengthArr: IntArrayList = null

  override def initialize(initialSize: Int) {
    _arr = new ByteArrayList(initialSize * ColumnIterator.STRING_SIZE)
    _lengthArr = new IntArrayList(initialSize)
    _stats = new ColumnStats.StringColumnStats
    super.initialize(initialSize)
  }

  override def append(o: Object, oi: ObjectInspector) {
    if (o == null) {
      appendNull()
    } else {
      val v = oi.asInstanceOf[StringObjectInspector].getPrimitiveWritableObject(o)
      append(v)
    }
  }

  override def append(v: Text) {
    _lengthArr.add(v.getLength)
    _arr.addElements(_arr.size, v.getBytes, 0, v.getLength)
    _stats.append(v)
  }

  override def appendNull() {
    _lengthArr.add(NULL_VALUE)
    _stats.appendNull()
  }

  override def stats = _stats

  override def build: ByteBuffer = {
    val buf = ByteBuffer.allocate(
      _lengthArr.size * 4 + _arr.size + ColumnIterator.COLUMN_TYPE_LENGTH)
    buf.order(ByteOrder.nativeOrder())
    buf.putLong(ColumnIterator.STRING)
    var i = 0
    var runningOffset = 0
    while (i < _lengthArr.size) {
      val len = _lengthArr.get(i)
      buf.putInt(len)

      if (NULL_VALUE != len) {
        buf.put(_arr.elements(), runningOffset, len)
        runningOffset += len
      }

      i += 1
    }
    buf.rewind()
    buf
  }
}

