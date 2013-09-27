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

import org.apache.hadoop.io.BooleanWritable

import shark.memstore2.column.Implicits._

/**
 * Iterates through a byte buffer containing compressed data.
 * The first element of the buffer at the point of initialization
 * is expected to be the type of compression indicator.
 */
trait CompressedColumnIterator extends ColumnIterator {

  private var _decoder: Iterator[_] = _
  private var _current: Any = _

  def buffer: ByteBuffer

  def columnType: ColumnType[_,_]

  override def init() {
    val compressionType: CompressionType = buffer.getInt()
    _decoder = compressionType match {
      case DefaultCompressionType => new DefaultDecoder(buffer, columnType)
      case RLECompressionType => new RLDecoder(buffer, columnType)
      case DictionaryCompressionType => new DictDecoder(buffer, columnType)
      case BooleanBitSetCompressionType => new BooleanBitSetDecoder(buffer, columnType)
      case _ => throw new UnsupportedOperationException()
    }
  }

  override def next() {
    // TODO: can we remove the if branch?
    if (_decoder.hasNext) {
      _current = _decoder.next()
    }
  }

  override def hasNext = _decoder.hasNext

  override def current = _current.asInstanceOf[Object]
}

/**
 * Default representation of a Decoder. In this case the underlying buffer
 * has uncompressed data
 */
class DefaultDecoder[V](buffer: ByteBuffer, columnType: ColumnType[_, V]) extends Iterator[V] {
  private val _current: V = columnType.newWritable()

  override def hasNext = buffer.hasRemaining()

  override def next(): V = {
    columnType.extractInto(buffer, _current)
    _current
  }
}

/**
 * Run Length Decoder, decodes data compressed in RLE format of [element, length]
 */
class RLDecoder[V](buffer: ByteBuffer, columnType: ColumnType[_, V]) extends Iterator[V] {

  private var _run: Int = _
  private var _count: Int = 0
  private val _current: V = columnType.newWritable()

  override def hasNext = buffer.hasRemaining()

  override def next(): V = {
    if (_count == _run) {
      //next run
      columnType.extractInto(buffer, _current)
      _run = buffer.getInt()
      _count = 1
    } else {
      _count += 1
    }
    _current
  }
}

/**
 * Dictionary encoding compression.
 */
class DictDecoder[V](buffer: ByteBuffer, columnType: ColumnType[_, V]) extends Iterator[V] {

  // Dictionary in the form of an array. The index is the encoded value, and the value is the
  // decompressed value.
  private val _dictionary: Array[V] =  {
    val size = buffer.getInt()
    val arr = columnType.writableManifest.newArray(size)
    var count = 0
    while (count < size) {
      val writable = columnType.newWritable()
      columnType.extractInto(buffer, writable)
      arr(count) = writable.asInstanceOf[V]
      count += 1
    }
    arr
  }

  override def hasNext = buffer.hasRemaining()

  override def next(): V = {
    val index = buffer.getShort().toInt
    _dictionary(index)
  }
}

/**
 * Boolean BitSet encoding.
 */
class BooleanBitSetDecoder[V](
    buffer: ByteBuffer,
    columnType: ColumnType[_, V],
    var _pos: Int,
    var _uncompressedSize: Int,
    var _curValue: Long,
    var _writable: BooleanWritable
  ) extends Iterator[V] {

  def this(buffer: ByteBuffer, columnType: ColumnType[_, V])
      = this(buffer, columnType, 0, buffer.getInt(), 0, new BooleanWritable())

  override def hasNext = _pos < _uncompressedSize

  override def next(): V = {
    val offset = _pos % BooleanBitSetCompression.BOOLEANS_PER_LONG

    if (offset == 0) {
      _curValue = buffer.getLong()
    }

    val retval: Boolean = (_curValue & (1 << offset)) != 0
    _pos += 1
    _writable.set(retval)
    _writable.asInstanceOf[V]
  }
}

