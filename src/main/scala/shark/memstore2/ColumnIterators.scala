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


/**
 * A mapping from an integer column type to the respective ColumnIterator class.
 */
object ColumnIterators {

  type IteratorType = Int

  val COLUMN_TYPE_LENGTH = 4

  private val _iteratorClass = new Array[Class[_ <: ColumnIterator]](1200)

  def getIteratorClass(columnType: IteratorType): Class[_ <: ColumnIterator] = {
    _iteratorClass(columnType)
  }

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

  val LAZY = 900
  //_iteratorClass(LAZY) = classOf[LazyColumnIterator]

  val LAZY_NULLABLE = 901

  val TIMESTAMP = 1000
  //_iteratorClass(TIMESTAMP) = classOf[TimestampColumnIterator]

  val TIMESTAMP_NULLABLE = 1001

  val BINARY = 1100
  //_iteratorClass(BINARY) = classOf[BinaryColumnIterator]

  val BINARY_NULLABLE = 1101

}