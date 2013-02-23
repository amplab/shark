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

package shark.memstore2

import java.nio.ByteBuffer
import java.nio.ByteOrder

import it.unimi.dsi.fastutil.booleans.BooleanArrayList
import it.unimi.dsi.fastutil.bytes.ByteArrayList
import it.unimi.dsi.fastutil.chars.CharArrayList
import it.unimi.dsi.fastutil.doubles.DoubleArrayList
import it.unimi.dsi.fastutil.floats.FloatArrayList
import it.unimi.dsi.fastutil.ints.IntArrayList
import it.unimi.dsi.fastutil.longs.LongArrayList
import it.unimi.dsi.fastutil.shorts.ShortArrayList

import javaewah.EWAHCompressedBitmap

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.{BooleanObjectInspector,
  ByteObjectInspector, ShortObjectInspector, IntObjectInspector, LongObjectInspector,
  FloatObjectInspector, DoubleObjectInspector, StringObjectInspector}
import org.apache.hadoop.io.Text


trait ColumnBuilder[T] {
  protected var _nulls: EWAHCompressedBitmap = null

  def initialize(initialSize: Int) {
    _nulls = new EWAHCompressedBitmap
  }

  def append(o: Object, oi: ObjectInspector)

  def append(v: T)

  def appendNull()

  def build: ByteBuffer

}


class BooleanColumnBuilder extends ColumnBuilder[Boolean] {
  private var _arr: BooleanArrayList = null

  override def initialize(initialSize: Int) {
    _arr = new BooleanArrayList(initialSize)
    super.initialize(initialSize)
  }

  override def append(o: Object, oi: ObjectInspector) {
    if (o == null) {
      appendNull()
    } else {
      val v = oi.asInstanceOf[BooleanObjectInspector].get(o)
      append(v)
    }
  }

  override def append(v: Boolean) {
    _arr.add(v)
  }

  override def appendNull() {
    _nulls.set(_arr.size)
    _arr.add(false)
  }

  override def build: ByteBuffer = {
    // TODO: This only supports non-null iterators.
    val buffer = ByteBuffer.allocate(_arr.size + ColumnIterators.COLUMN_TYPE_LENGTH)
    buffer.order(ByteOrder.nativeOrder())
    buffer.putInt(ColumnIterators.BOOLEAN)
    var i = 0
    while (i < _arr.size) {
      buffer.put(if (_arr.get(i)) 1.toByte else 0.toByte)
      i += 1
    }
    buffer.rewind()
    buffer
  }
}


class ByteColumnBuilder extends ColumnBuilder[Byte] {
  private var _arr: ByteArrayList = null
  private val zero: Byte = 0

  override def initialize(initialSize: Int) {
    _arr = new ByteArrayList(initialSize)
    super.initialize(initialSize)
  }

  override def append(o: Object, oi: ObjectInspector) {
    if (o == null) {
      appendNull()
    } else {
      val v = oi.asInstanceOf[ByteObjectInspector].get(o)
      append(v)
    }
  }

  override def append(v: Byte) {
    _arr.add(v)
  }

  override def appendNull() {
    _nulls.set(_arr.size)
    _arr.add(zero)
  }

  override def build: ByteBuffer = {
    // TODO: This only supports non-null iterators.
    val buffer = ByteBuffer.allocate(_arr.size + ColumnIterators.COLUMN_TYPE_LENGTH)
    buffer.order(ByteOrder.nativeOrder())
    buffer.putInt(ColumnIterators.BYTE)
    var i = 0
    while (i < _arr.size) {
      buffer.put(_arr.get(i))
      i += 1
    }
    buffer.rewind()
    buffer
  }
}


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
    val buffer = ByteBuffer.allocate(_arr.size * 2 + ColumnIterators.COLUMN_TYPE_LENGTH)
    buffer.order(ByteOrder.nativeOrder())
    buffer.putInt(ColumnIterators.SHORT)
    var i = 0
    while (i < _arr.size) {
      buffer.putShort(_arr.get(i))
      i += 1
    }
    buffer.rewind()
    buffer
  }
}


class IntColumnBuilder extends ColumnBuilder[Int] {
  private var _arr: IntArrayList = null

  override def initialize(initialSize: Int) {
    _arr = new IntArrayList(initialSize)
    super.initialize(initialSize)
  }

  override def append(o: Object, oi: ObjectInspector) {
    if (o == null) {
      appendNull()
    } else {
      val v = oi.asInstanceOf[IntObjectInspector].get(o)
      append(v)
    }
  }

  override def append(v: Int) {
    _arr.add(v)
  }

  override def appendNull() {
    _nulls.set(_arr.size)
    _arr.add(0)
  }

  override def build: ByteBuffer = {
    // TODO: This only supports non-null iterators.
    val buffer = ByteBuffer.allocate(_arr.size * 4 + ColumnIterators.COLUMN_TYPE_LENGTH)
    buffer.order(ByteOrder.nativeOrder())
    buffer.putInt(ColumnIterators.INT)
    var i = 0
    while (i < _arr.size) {
      buffer.putInt(_arr.get(i))
      i += 1
    }
    buffer.rewind()
    buffer
  }
}


class LongColumnBuilder extends ColumnBuilder[Long] {
  private var _arr: LongArrayList = null

  override def initialize(initialSize: Int) {
    _arr = new LongArrayList(initialSize)
    super.initialize(initialSize)
  }

  override def append(o: Object, oi: ObjectInspector) {
    if (o == null) {
      appendNull()
    } else {
      val v = oi.asInstanceOf[LongObjectInspector].get(o)
      append(v)
    }
  }

  override def append(v: Long) {
    _arr.add(v)
  }

  override def appendNull() {
    _nulls.set(_arr.size)
    _arr.add(0)
  }

  override def build: ByteBuffer = {
    // TODO: This only supports non-null iterators.
    val buffer = ByteBuffer.allocate(_arr.size * 8 + ColumnIterators.COLUMN_TYPE_LENGTH)
    buffer.order(ByteOrder.nativeOrder())
    buffer.putInt(ColumnIterators.LONG)
    var i = 0
    while (i < _arr.size) {
      buffer.putLong(_arr.get(i))
      i += 1
    }
    buffer.rewind()
    buffer
  }
}


class FloatColumnBuilder extends ColumnBuilder[Float] {
  private var _arr: FloatArrayList = null

  override def initialize(initialSize: Int) {
    _arr = new FloatArrayList(initialSize)
    super.initialize(initialSize)
  }

  override def append(o: Object, oi: ObjectInspector) {
    if (o == null) {
      appendNull()
    } else {
      val v = oi.asInstanceOf[FloatObjectInspector].get(o)
      append(v)
    }
  }

  override def append(v: Float) {
    _arr.add(v)
  }

  override def appendNull() {
    _nulls.set(_arr.size)
    _arr.add(0)
  }

  override def build: ByteBuffer = {
    // TODO: This only supports non-null iterators.
    val buffer = ByteBuffer.allocate(_arr.size * 4 + ColumnIterators.COLUMN_TYPE_LENGTH)
    buffer.order(ByteOrder.nativeOrder())
    buffer.putInt(ColumnIterators.FLOAT)
    var i = 0
    while (i < _arr.size) {
      buffer.putFloat(_arr.get(i))
      i += 1
    }
    buffer.rewind()
    buffer
  }
}


class DoubleColumnBuilder extends ColumnBuilder[Double] {
  private var _arr: DoubleArrayList = null

  override def initialize(initialSize: Int) {
    _arr = new DoubleArrayList(initialSize)
    super.initialize(initialSize)
  }

  override def append(o: Object, oi: ObjectInspector) {
    if (o == null) {
      appendNull()
    } else {
      val v = oi.asInstanceOf[DoubleObjectInspector].get(o)
      append(v)
    }
  }

  override def append(v: Double) {
    _arr.add(v)
  }

  override def appendNull() {
    _nulls.set(_arr.size)
    _arr.add(0)
  }

  override def build: ByteBuffer = {
    // TODO: This only supports non-null iterators.
    val buffer = ByteBuffer.allocate(_arr.size * 8 + ColumnIterators.COLUMN_TYPE_LENGTH)
    buffer.order(ByteOrder.nativeOrder())
    buffer.putInt(ColumnIterators.DOUBLE)
    var i = 0
    while (i < _arr.size) {
      buffer.putDouble(_arr.get(i))
      i += 1
    }
    buffer.rewind()
    buffer
  }
}


class StringColumnBuilder extends ColumnBuilder[Text] {

  private val NULL_VALUE = -1
  private var _arr: ByteArrayList = null
  private var _lengthArr: IntArrayList = null

  override def initialize(initialSize: Int) {
    // TODO: Change initial size for byte array.
    _arr = new ByteArrayList(initialSize)
    _lengthArr = new IntArrayList(initialSize)
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
  }

  override def appendNull() {
    _lengthArr.add(NULL_VALUE)
  }

  override def build: ByteBuffer = {
    // TODO: This only supports non-null iterators.
    val buffer = ByteBuffer.allocate(
      _lengthArr.size * 4 + _arr.size + ColumnIterators.COLUMN_TYPE_LENGTH)
    buffer.order(ByteOrder.nativeOrder())
    buffer.putInt(ColumnIterators.STRING)
    var i = 0
    var runningOffset = 0
    while (i < _lengthArr.size) {
      val len = _lengthArr.get(i)
      buffer.putInt(len)

      if (NULL_VALUE != len) {
        buffer.put(_arr.elements(), runningOffset, len)
        runningOffset += len
      }

      i += 1
    }
    buffer.rewind()
    buffer
  }
}

// TODO: Add column builder for
// Void
// Lazy format