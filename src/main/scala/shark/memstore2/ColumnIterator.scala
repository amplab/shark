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

import java.lang.StringBuilder
import java.nio.ByteBuffer
import java.nio.ByteOrder

import org.apache.hadoop.hive.serde2.io.{ByteWritable, DoubleWritable, ShortWritable}
import org.apache.hadoop.io.{BooleanWritable, IntWritable, LongWritable, FloatWritable, Text,
  NullWritable}


/**
 * Iterator interface for a column. The iterator should be initialized by a byte
 * buffer, and next can be invoked to get the value for each cell.
 */
trait ColumnIterator {
  protected var _bytes: ByteBuffer = null

  def initialize(bytes: ByteBuffer) {
    bytes.order(ByteOrder.nativeOrder())
    _bytes = bytes
  }

  def next: Object

  def current: Object
}


class BooleanColumnIterator extends ColumnIterator {
  // TODO: Use a single bit per boolean value.
  private val _writable = new BooleanWritable

  override def next: Object = {
    _writable.set(_bytes.get != 0)
    _writable
  }

  override def current = _writable
}


class ByteColumnIterator extends ColumnIterator {
  private val _writable = new ByteWritable

  override def next: Object = {
    _writable.set(_bytes.get)
    _writable
  }

  override def current = _writable
}


class ShortColumnIterator extends ColumnIterator {
  private val _writable = new ShortWritable

  override def next: Object = {
    _writable.set(_bytes.getShort)
    _writable
  }

  override def current = _writable
}


class IntColumnIterator extends ColumnIterator {
  private val _writable = new IntWritable

  override def next: Object = {
    _writable.set(_bytes.getInt)
    _writable
  }

  override def current = _writable
}


class LongColumnIterator extends ColumnIterator {
  private val _writable = new LongWritable

  override def next: Object = {
    _writable.set(_bytes.getLong)
    _writable
  }

  override def current = _writable
}


class FloatColumnIterator extends ColumnIterator {
  private val _writable = new FloatWritable

  override def next: Object = {
    _writable.set(_bytes.getFloat)
    _writable
  }

  override def current = _writable
}


class DoubleColumnIterator extends ColumnIterator {
  private val _writable = new DoubleWritable

  override def next: Object = {
    _writable.set(_bytes.getDouble)
    _writable
  }

  override def current = _writable
}


class VoidColumnIterator extends ColumnIterator {
  private val _writable = NullWritable.get()
  override def next: Object = _writable
  override def current = _writable
}


class StringColumnIterator extends ColumnIterator {
  private val _writable = new Text

  override def next: Object = {
    val length = _bytes.getInt
    val stringBuilder = new StringBuilder()
    for (i <- 0 until length) {
      stringBuilder.append(_bytes.getChar)
    }
    _writable.set(stringBuilder.toString())
    _writable
  }

  override def current = _writable
}

// TODO: Add column iterators for
// Lazy format

// TODO: Add nullable column iterators.

// TODO: Add compression column iterators.

