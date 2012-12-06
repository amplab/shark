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

package shark.memstore

import it.unimi.dsi.fastutil.ints.IntArrayList
import javaewah.{EWAHCompressedBitmap, IntIterator}
import org.apache.hadoop.io.IntWritable


class CompressedIntColumnFormat(initialSize: Int) extends NullBitmapColumnFormat[Int] {

  val nulls = new EWAHCompressedBitmap
  val stats = new ColumnStats.IntColumnStats
  val arr = new IntArrayList(initialSize)

  override def size: Int = { throw new UnsupportedOperationException }

  override def append(v: Int) {
    //stats.append(v)
    arr.add(v)
  }

  override def appendNull() {
    //stats.appendNull()
    nulls.set(arr.size)
    arr.add(0)
  }

  override def build: ColumnFormat[Int] = {
    arr.trim
    val range = stats.max - stats.min
    // Only compress if there is actually any data in it.
    if (arr.size - nulls.cardinality == 0 || range > CompressedIntColumnFormat.SHORT_RANGE) {
      new CompressedIntColumnFormat.Compressed(
        new CompressedIntColumnFormat.IntArray(arr.elements), nulls)
    } else if (range > CompressedIntColumnFormat.BYTE_RANGE) {
      new CompressedIntColumnFormat.Compressed(
        new CompressedIntColumnFormat.ShortArrayAsIntArray(arr.elements, stats.min, stats.max),
        nulls)
    } else {
      new CompressedIntColumnFormat.Compressed(
        new CompressedIntColumnFormat.ByteArrayAsIntArray(arr.elements, stats.min, stats.max),
        nulls)
    }
  }

  override def iterator: ColumnFormatIterator = { throw new UnsupportedOperationException }
}

object CompressedIntColumnFormat {

  val BYTE_RANGE = Byte.MaxValue - Byte.MinValue
  val SHORT_RANGE = Short.MaxValue - Short.MinValue

  class Compressed(val arr: BackingIntArray, val nulls: EWAHCompressedBitmap)
    extends NullBitmapColumnFormat[Int] {

    override def size: Int = arr.size
    override def append(v: Int) { throw new UnsupportedOperationException }
    override def appendNull() { throw new UnsupportedOperationException }
    override def build: ColumnFormat[Int] = { throw new UnsupportedOperationException }

    override def iterator: ColumnFormatIterator = {
      new NullBitmapColumnIterator[Int](this) {
        val writable = new IntWritable
        override def getObject(i: Int): Object = {
          writable.set(arr(i))
          writable
        }
      }
    }
  }

  trait BackingIntArray {
    def apply(i: Int): Int
    def size: Int
  }

  class ByteArrayAsIntArray(intArray: Array[Int], min: Int, max: Int) extends BackingIntArray {
    def this(intArray: Array[Int]) = this(intArray, intArray.min, intArray.max)

    override def size: Int = _data.size
    override def apply(i: Int): Int = _data(i) + base
    val base = (min + max) / 2

    private val _data = new Array[Byte](intArray.size)
    private def init(intArray: Array[Int], min: Int, max: Int) {
      var i = 0
      while (i < intArray.size) {
        _data(i) = (intArray(i) - base).toByte
        i+= 1
      }
    }
    init(intArray, min, max)
  }

  class ShortArrayAsIntArray(intArray: Array[Int], min: Int, max: Int) extends BackingIntArray {
    def this(intArray: Array[Int]) = this(intArray, intArray.min, intArray.max)

    override def size: Int = _data.size
    override def apply(i: Int): Int = _data(i) + base
    val base = (min + max) / 2

    private val _data = new Array[Short](intArray.size)
    private def init(intArray: Array[Int], min: Int, max: Int) {
      var i = 0
      while (i < intArray.size) {
        _data(i) = (intArray(i) - base).toShort
        i+= 1
      }
    }
    init(intArray, min, max)
  }

  class IntArray(intArray: Array[Int]) extends BackingIntArray {
    override def apply(i: Int): Int = intArray(i)
    override def size: Int = intArray.size
  }
}
