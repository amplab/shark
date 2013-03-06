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

import shark.memstore2.buffer.ByteBufferReader


/**
 * Iterator interface for a column. The iterator should be initialized by a byte
 * buffer, and next can be invoked to get the value for each cell.
 */
trait ColumnIterator {
  protected var _bytesReader: ByteBufferReader = null

  def initialize(bytes: ByteBuffer) {
    _bytesReader = ByteBufferReader.createUnsafeReader(bytes)
    // Skip the first few bytes, which is metadata for the column type.
    _bytesReader.position(ColumnIterator.COLUMN_TYPE_LENGTH)
  }

  def next: Object

  def current: Object
}


/**
 * A mapping from an integer column type to the respective ColumnIterator class.
 */
object ColumnIterator {

  // TODO: Implement Decimal data type.

  type IteratorType = Int

  val COLUMN_TYPE_LENGTH = 8

  private val _iteratorClass = new Array[Class[_ <: ColumnIterator]](2000)

  def getIteratorClass(columnType: IteratorType): Class[_ <: ColumnIterator] = {
    _iteratorClass(columnType)
  }

  /////////////////////////////////////////////////////////////////////////////
  // List of data type sizes.
  /////////////////////////////////////////////////////////////////////////////
  val BOOLEAN_SIZE = 1
  val BYTE_SIZE = 1
  val SHORT_SIZE = 2
  val INT_SIZE = 4
  val LONG_SIZE = 8
  val FLOAT_SIZE = 4
  val DOUBLE_SIZE = 8
  val TIMESTAMP_SIZE = 12

  // Strings, binary types, and complex types (map, list) are assumed to be 16 bytes on average.
  val BINARY_SIZE = 16
  val STRING_SIZE = 8
  val COMPLEX_TYPE_SIZE = 16

  // Void columns are represented by NullWritable, which is a singleton.
  val NULL_SIZE = 0

  /////////////////////////////////////////////////////////////////////////////
  // List of column iterators.
  /////////////////////////////////////////////////////////////////////////////
  val BOOLEAN = 0
  _iteratorClass(BOOLEAN) = classOf[BooleanColumnIterator]

  val BOOLEAN_NULLABLE = 1

  val BYTE = 100
  _iteratorClass(BYTE) = classOf[ByteColumnIterator]

  val BYTE_NULLABLE = 101

  val SHORT = 200
  _iteratorClass(SHORT) = classOf[ShortColumnIterator]

  val SHORT_NULLABLE = 201

  val INT = 300
  _iteratorClass(INT) = classOf[IntColumnIterator]

  val INT_NULLABLE = 301

  val LONG = 400
  _iteratorClass(LONG) = classOf[LongColumnIterator]

  val LONG_NULLABLE = 401

  val FLOAT = 500
  _iteratorClass(FLOAT) = classOf[FloatColumnIterator]

  val FLOAT_NULLABLE = 501

  val DOUBLE = 600
  _iteratorClass(DOUBLE) = classOf[DoubleColumnIterator]

  val DOUBLE_NULLABLE = 601

  val VOID = 700
  _iteratorClass(VOID) = classOf[VoidColumnIterator]

  val STRING = 800
  _iteratorClass(STRING) = classOf[StringColumnIterator]

  val STRING_NULLABLE = 801

  val COMPLEX = 900
  _iteratorClass(COMPLEX) = classOf[ComplexColumnIterator]

  val TIMESTAMP = 1000
  _iteratorClass(TIMESTAMP) = classOf[TimestampColumnIterator]

  val TIMESTAMP_NULLABLE = 1001

  val BINARY = 1100
  //_iteratorClass(BINARY) = classOf[BinaryColumnIterator]

  val BINARY_NULLABLE = 1101
}
