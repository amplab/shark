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

import it.unimi.dsi.fastutil.shorts.ShortArrayList

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ShortObjectInspector


class ShortColumnBuilder extends ColumnBuilder[Short] {
  private var _arr: ShortArrayList = null
  private val zero: Short = 0

  override def initialize(initialSize: Int) {
    _arr = new ShortArrayList(initialSize)
    super.initialize(initialSize)
  }

  override def append(o: Object, oi: ObjectInspector) {
    if (o == null) {
      appendNull()
    } else {
      val v = oi.asInstanceOf[ShortObjectInspector].get(o)
      append(v)
    }
  }

  override def append(v: Short) {
    _arr.add(v)
  }

  override def appendNull() {
    _nulls.set(_arr.size)
    _arr.add(zero)
  }

  override def build: ByteBuffer = {
    // TODO: This only supports non-null iterators.
    val buf = ByteBuffer.allocate(_arr.size * 2 + ColumnIterator.COLUMN_TYPE_LENGTH)
    buf.order(ByteOrder.nativeOrder())
    buf.putLong(ColumnIterator.SHORT)
    var i = 0
    while (i < _arr.size) {
      buf.putShort(_arr.get(i))
      i += 1
    }
    buf.rewind()
    buf
  }
}