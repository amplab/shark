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
import shark.LogHelper

/**
 * Factory class used to create column iterators for a given data type.
 * The implementation of this trait can determine the specific implementation
 * of the column iterator to create depending on metadata embedded in the
 * buffer. For example, it can choose whether to handle null values or not.
 */
trait ColumnIteratorFactory extends LogHelper{
  def createIterator(buf: ByteBufferReader): ColumnIterator
}


object ColumnIteratorFactory{

  /**
   * Create a factory for the given column iterator.
   */
  def create[T <: ColumnIterator](baseIterClass: Class[T]): ColumnIteratorFactory = {
    new ColumnIteratorFactory {
      override def createIterator(buf: ByteBufferReader): ColumnIterator = {
        val ctor = baseIterClass.getConstructor(classOf[ByteBufferReader])
        ctor.newInstance(buf).asInstanceOf[T]
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
        if (nullable) {
          logDebug("baseIterClass " + baseIterClass + " created with EWAH")
          new EWAHNullableColumnIterator[T](baseIterClass, buf)
        } else {
          val ctor = baseIterClass.getConstructor(classOf[ByteBufferReader])
          ctor.newInstance(buf).asInstanceOf[T]
        }
      }
    }
  }


  /**
   * Create a factory for a column iterator wrapped by Run Length encoding 
   */
  def createWithRLE[T <: ColumnIterator](baseIterClass: Class[T]): ColumnIteratorFactory = {
    new ColumnIteratorFactory {
      override def createIterator(buf: ByteBufferReader): ColumnIterator = {
          logDebug("baseIterClass " + baseIterClass + " created with RLE")
          new RLEColumnIterator[T](baseIterClass, buf)
      }
    }
  }

  /**
   * Create a factory for an int column iterator wrapped by RLE first and then EWAH
   */
  def createIntWithEwahRLE: ColumnIteratorFactory = {
    new ColumnIteratorFactory {
      override def createIterator(buf: ByteBufferReader): ColumnIterator = {

        logInfo("IntColumnIterator " + " created with RLE & EWAH")
        val nullable = (buf.getLong() == 1L)
        if (nullable) {
          // funky 3 layer construction required because bytebuffer advances automatically
          val iter = new RLEColumnIterator(classOf[IntColumnIterator.Default])
          val ret = new EWAHNullableColumnIterator(iter, buf)
          iter.initialize(buf)
          ret
        } else {
          new RLEColumnIterator(classOf[IntColumnIterator.Default], buf)
        }
      }
    }
  }

  /**
   * Create a factory for a column iterator wrapped by LZF compression
   */
  def createWithLZF[T <: ColumnIterator](baseIterClass: Class[T]): ColumnIteratorFactory = {
    new ColumnIteratorFactory {
      override def createIterator(buf: ByteBufferReader): ColumnIterator = {
          logInfo("baseIterClass " + baseIterClass + " created with LZF")
          new LZFBlockColumnIterator[T](baseIterClass, buf)
//        new LZFColumnIterator[T](baseIterClass, buf)
      }
    }
  }
 
/*
  def createWithEWAHDecorator(c: ColumnIterator) = {
    new ColumnIteratorFactory {
      override def createIterator(buf: ByteBufferReader): ColumnIterator = {
        val nullable = buf.getLong() == 1L
        if (nullable) {
          logDebug(
            " created with EWAHDecorator")
// troubles mixing mixins and reflection in scala
          new (classOf[c].getInstance(buf) with EWAHNullableColumnIteratorDecorator)
        } else {
          c
        }
      }
    }
  }
*/
}
