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

import shark.memstore2.buffer.ByteBufferReader


/**
 * Iterator interface for a column. The iterator should be initialized by a byte
 * buffer, and next can be invoked to get the value for each cell.
 */
abstract class ColumnIterator {

  def next()

  def current: Object
}


/**
 * A mapping from an integer column type to the respective ColumnIterator class.
 */
object ColumnIterator {

  // TODO: Implement Decimal data type.

  val COLUMN_TYPE_LENGTH = 8

  // A mapping between column type to the column iterator factory.
  private val _iteratorFactory = new Array[ColumnIteratorFactory](32)

  def getFactory(columnType: Long): ColumnIteratorFactory = {
    _iteratorFactory(columnType.toInt)
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
  _iteratorFactory(BOOLEAN) = createFactoryWithEWAH(classOf[BooleanColumnIterator.Default])

  val BYTE = 1
  _iteratorFactory(BYTE) = createFactoryWithEWAH(classOf[ByteColumnIterator.Default])

  val SHORT = 2
  _iteratorFactory(SHORT) = createFactoryWithEWAH(classOf[ShortColumnIterator.Default])

  val INT = 3
  _iteratorFactory(INT) = createFactoryWithEWAH(classOf[IntColumnIterator.Default])

  val LONG = 4
  _iteratorFactory(LONG) = createFactoryWithEWAH(classOf[LongColumnIterator.Default])

  val FLOAT = 5
  _iteratorFactory(FLOAT) = createFactoryWithEWAH(classOf[FloatColumnIterator.Default])

  val DOUBLE = 6
  _iteratorFactory(DOUBLE) = createFactoryWithEWAH(classOf[DoubleColumnIterator.Default])

  val VOID = 7
  _iteratorFactory(VOID) = createFactory(classOf[VoidColumnIterator.Default])

  val TIMESTAMP = 8
  _iteratorFactory(TIMESTAMP) = createFactoryWithEWAH(classOf[TimestampColumnIterator.Default])

  // TODO: Add decimal data type.

  val STRING = 10
  _iteratorFactory(STRING) = createFactory(classOf[StringColumnIterator.Default])

  val COMPLEX = 11
  _iteratorFactory(COMPLEX) = createFactory(classOf[ComplexColumnIterator.Default])

  val BINARY = 12
  _iteratorFactory(BINARY) = createFactory(classOf[BinaryColumnIterator.Default])

  val DICT_INT = 13
  _iteratorFactory(DICT_INT) = createFactoryWithEWAH(classOf[DictionaryEncodedIntColumnIterator.Default])

  // Helper methods so we don't need to write the whole thing up there.
  def createFactory[T <: ColumnIterator](c: Class[T]) = {
    ColumnIteratorFactory.create(c)
  }

  def createFactoryWithEWAH[T <: ColumnIterator](c: Class[T]) = {
    ColumnIteratorFactory.createWithEWAH(c)
  }

}
