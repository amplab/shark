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
    val buffer = ByteBuffer.allocate(_arr.size)
    var i = 0
    while (i < _arr.size) {
      buffer.put(if (_arr.get(i)) 1.toByte else 0.toByte)
      i += 1
    }
    buffer.rewind()
    buffer
  }
}


// TODO: Add column builder for
// Short
// Long
// Float
// Double
// String
// Void
// Lazy format


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
    val buffer = ByteBuffer.allocate(_arr.size * 4)
    buffer.order(ByteOrder.nativeOrder())
    var i = 0
    while (i < _arr.size) {
      buffer.putInt(_arr.get(i))
      i += 1
    }
    buffer.rewind()
    buffer
  }
}
