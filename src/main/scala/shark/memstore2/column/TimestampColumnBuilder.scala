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
import java.sql.Timestamp

import it.unimi.dsi.fastutil.ints.IntArrayList
import it.unimi.dsi.fastutil.longs.LongArrayList

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector


/**
 * Timestamp column. A timestamp consists a long value and an int value.
 */
class TimestampColumnBuilder extends ColumnBuilder[Timestamp] {

  private var _arrTime: LongArrayList = null
  private var _arrNanos: IntArrayList = null

  private var _hasNanoField = false

  override def initialize(initialSize: Int) {
    _arrTime = new LongArrayList(initialSize)
    _arrNanos = new IntArrayList(initialSize)
    _hasNanoField = false
    super.initialize(initialSize)
  }

  override def append(o: Object, oi: ObjectInspector) {
    if (o == null) {
      appendNull()
    } else {
      val v: Timestamp = oi.asInstanceOf[TimestampObjectInspector].getPrimitiveJavaObject(o)
      append(v)
    }
  }

  override def append(v: Timestamp) {
    _arrTime.add(v.getTime())
    val nano = v.getNanos()
    if (nano != 0) {
      _hasNanoField = true
    }
    _arrNanos.add(nano)
  }

  override def appendNull() {
    _nulls.set(_arrTime.size)
    _arrTime.add(0)
    _arrNanos.add(0)
  }

  override def build: ByteBuffer = {
    // TODO: This only supports non-null iterators.
    // TODO: As an optimization, we can optionally skip nanos field if it is not used.
    val buf = ByteBuffer.allocate(
      _arrTime.size * 8 + _arrNanos.size * 4 + ColumnIterator.COLUMN_TYPE_LENGTH)
    buf.order(ByteOrder.nativeOrder())
    buf.putLong(ColumnIterator.TIMESTAMP)
    var i = 0
    while (i < _arrTime.size) {
      buf.putLong(_arrTime.get(i))
      buf.putInt(_arrNanos.get(i))
      i += 1
    }
    buf.rewind()
    buf
  }
}