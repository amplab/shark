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
 * Factory class used to create column iterators for a given data type.
 * The implementation of this trait can determine the specific implementation
 * of the column iterator to create depending on metadata embedded in the
 * buffer. For example, it can choose whether to handle null values or not.
 */
trait ColumnIteratorFactory {
  def createIterator(buf: ByteBufferReader): ColumnIterator
}


object ColumnIteratorFactory {

  /**
   * Create a factory for the given column iterator.
   */
  def create[T <: ColumnIterator](baseIterClass: Class[T]): ColumnIteratorFactory = {
    new ColumnIteratorFactory {
      override def createIterator(buf: ByteBufferReader): ColumnIterator = {
        val iter = baseIterClass.newInstance.asInstanceOf[T]
        iter.initialize(buf)
        iter
      }
    }
  }

  /**
   * Create a factory for a column iterator wrapped by a EWAH bitmap for null values.
   */
  def createWithEWAH[T <: ColumnIterator](baseIterClass: Class[T]): ColumnIteratorFactory = {
    new ColumnIteratorFactory {
      override def createIterator(buf: ByteBufferReader): ColumnIterator = {
        val nullable = buf.getLong() == 1L
        val baseIter = baseIterClass.newInstance.asInstanceOf[T]
        val iter = if (nullable) new EWAHNullableColumnIterator[T](baseIter) else baseIter
        iter.initialize(buf)
        iter
      }
    }
  }
}
