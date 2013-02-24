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

import it.unimi.dsi.fastutil.booleans.BooleanArrayList
import it.unimi.dsi.fastutil.bytes.ByteArrayList
import it.unimi.dsi.fastutil.doubles.DoubleArrayList
import it.unimi.dsi.fastutil.floats.FloatArrayList
import it.unimi.dsi.fastutil.ints.IntArrayList
import it.unimi.dsi.fastutil.longs.LongArrayList
import it.unimi.dsi.fastutil.shorts.ShortArrayList
import javaewah.EWAHCompressedBitmap
import org.apache.hadoop.hive.serde2.ByteStream
import org.apache.hadoop.hive.serde2.io.{ByteWritable, DoubleWritable, ShortWritable}
import org.apache.hadoop.hive.serde2.`lazy`.{ByteArrayRef, LazyFactory, LazyObject}
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.io.{BooleanWritable, IntWritable, LongWritable, FloatWritable, Text,
  NullWritable}
import org.apache.hadoop.hive.serde2.`lazy`.LazyBinary
import org.apache.hadoop.hive.serde2.`lazy`.objectinspector.primitive.LazyBinaryObjectInspector
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryBinary
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableBinaryObjectInspector
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryFactory
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryFactory
import java.sql.Timestamp
import org.apache.hadoop.hive.serde2.io.TimestampWritable
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryObject
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryNonPrimitive
import org.apache.hadoop.io.BytesWritable


trait UncompressedColumnFormat[T] extends NullBitmapColumnFormat[T] {
  val nulls = new EWAHCompressedBitmap
}


object UncompressedColumnFormat {

  class BooleanColumnFormat(initialSize: Int) extends UncompressedColumnFormat[Boolean] {
    val arr = new BooleanArrayList(initialSize)

    override def size: Int = arr.size
    override def append(v: Boolean) = arr.add(v)

    override def appendNull = {
      nulls.set(arr.size)
      arr.add(false)
    }

    override def build = {
      arr.trim
      this
    }

    override def iterator: ColumnFormatIterator = new NullBitmapColumnIterator[Boolean](this) {
      val writable = new BooleanWritable
      override def getObject(i: Int): Object = {
        writable.set(arr.getBoolean(i))
        writable
      }
    }
  }

  class ByteColumnFormat(initialSize: Int) extends UncompressedColumnFormat[Byte] {
    val arr = new ByteArrayList(initialSize)

    override def size: Int = arr.size
    override def append(v: Byte) = arr.add(v)

    override def appendNull = {
      nulls.set(arr.size)
      arr.add(0.toByte)
    }

    override def build = {
      arr.trim
      this
    }

    override def iterator: ColumnFormatIterator = new NullBitmapColumnIterator[Byte](this) {
      val writable = new ByteWritable
      override def getObject(i: Int): Object = {
        writable.set(arr.getByte(i))
        writable
      }
    }
  }

  class ShortColumnFormat(initialSize: Int) extends UncompressedColumnFormat[Short] {
    val arr = new ShortArrayList(initialSize)

    override def size: Int = arr.size
    override def append(v: Short) = arr.add(v)

    override def appendNull = {
      nulls.set(arr.size)
      arr.add(0.toShort)
    }

    override def build = {
      arr.trim
      this
    }

    override def iterator: ColumnFormatIterator = new NullBitmapColumnIterator[Short](this) {
      val writable = new ShortWritable
      override def getObject(i: Int): Object = {
        writable.set(arr.getShort(i))
        writable
      }
    }
  }

  class IntColumnFormat(initialSize: Int) extends UncompressedColumnFormat[Int] {
    val arr = new IntArrayList(initialSize)

    override def size: Int = arr.size
    override def append(v: Int) = arr.add(v)

    override def appendNull = {
      nulls.set(arr.size)
      arr.add(0)
    }

    override def build = {
      arr.trim
      this
    }

    override def iterator: ColumnFormatIterator = new NullBitmapColumnIterator[Int](this) {
      val writable = new IntWritable
      override def getObject(i: Int): Object = {
        writable.set(arr.getInt(i))
        writable
      }
    }
  }

  class LongColumnFormat(initialSize: Int) extends UncompressedColumnFormat[Long] {
    val arr = new LongArrayList(initialSize)

    override def size: Int = arr.size
    override def append(v: Long) = arr.add(v)

    override def appendNull = {
      nulls.set(arr.size)
      arr.add(0)
    }

    override def build = {
      arr.trim
      this
    }

    override def iterator: ColumnFormatIterator = new NullBitmapColumnIterator[Long](this) {
      val writable = new LongWritable
      override def getObject(i: Int): Object = {
        writable.set(arr.getLong(i))
        writable
      }
    }
  }

  class FloatColumnFormat(initialSize: Int) extends UncompressedColumnFormat[Float] {
    val arr = new FloatArrayList(initialSize)
    val writable = new FloatWritable

    override def size: Int = arr.size
    override def append(v: Float) = arr.add(v)

    override def appendNull = {
      nulls.set(arr.size)
      arr.add(0)
    }

    override def build = {
      arr.trim
      this
    }

    override def iterator: ColumnFormatIterator = new NullBitmapColumnIterator[Float](this) {
      val writable = new FloatWritable
      override def getObject(i: Int): Object = {
        writable.set(arr.getFloat(i))
        writable
      }
    }
  }

  class DoubleColumnFormat(initialSize: Int) extends UncompressedColumnFormat[Double] {
    val arr = new DoubleArrayList(initialSize)

    override def size: Int = arr.size
    override def append(v: Double) = arr.add(v)

    override def appendNull = {
      nulls.set(arr.size)
      arr.add(0)
    }

    override def build = {
      arr.trim
      this
    }

    override def iterator: ColumnFormatIterator = new NullBitmapColumnIterator[Double](this) {
      val writable = new DoubleWritable
      override def getObject(i: Int): Object = {
        writable.set(arr.getDouble(i))
        writable
      }
    }
  }

  class TextColumnFormat(initialSize: Int) extends ColumnFormat[Text] {
    // All strings are stored into a single byte array. Each string's length is
    // encoded in sizes array. If a string's size is negative, it means the
    // value is NULL.
    val arr = new ByteArrayList(initialSize * ColumnarSerDe.STRING_SIZE)
    val sizes = new IntArrayList(initialSize)
    sizes.add(0)

    override def size: Int = sizes.size - 1

    override def append(v: Text) = {
      sizes.add(v.getLength)
      arr.addElements(arr.size, v.getBytes, 0, v.getLength)
    }

    override def appendNull() {
      sizes.add(-1)
    }

    override def build = {
      arr.trim
      sizes.trim
      this
    }

    override def iterator: ColumnFormatIterator = new ColumnFormatIterator {
      var _position = 0
      var _offset = 0
      val writable = new Text
      override def nextRow() {
        val length = sizes.getInt(_position)
        if (length > 0) _offset += length
        _position += 1
      }
      override def current(): Object = {
        val length = sizes.getInt(_position)
        if (length >= 0) {
          writable.set(arr.elements, _offset, length)
          writable
        } else {
          null
        }
      }
    }
  }

  class VoidColumnFormat extends ColumnFormat[Void] {
    val void = NullWritable.get()
    private var _size = 0
    override def size: Int = _size
    override def append(v: Void) { _size += 1 }
    override def appendNull() { _size += 1 }
    override def build = this
    override def iterator: ColumnFormatIterator = new ColumnFormatIterator {
      override def nextRow() { }
      override def current(): Object = void
    }
  }

  // We expect data to be serialized before passing into the lazy column format.
  // As a result, it never expects NULLs.
  class LazyColumnFormat(outputOI: ObjectInspector, initialSize: Int)
    extends ColumnFormat[ByteStream.Output] {
    val arr = new ByteArrayList(initialSize * ColumnarSerDe.getFieldSize(outputOI))
    val ref = new ByteArrayRef()

    val sizes = new IntArrayList(initialSize)
    sizes.add(0)

    override def size: Int = sizes.size - 1

    override def append(v: ByteStream.Output) {
      sizes.add(v.getCount)
      arr.addElements(arr.size(), v.getData, 0, v.getCount)
    }

    override def appendNull =
      throw new UnsupportedOperationException("LazyColumnFormat.appendNull()")

    override def build = {
      // Not sure if we should make a copy of the array
      arr.trim
      sizes.trim
      ref.setData(arr.elements)
      this
    }

    override def iterator: ColumnFormatIterator = new ColumnFormatIterator {
      val o = LazyFactory.createLazyObject(outputOI)
      var _position = 0
      var _offset = 0
      override def nextRow() {
        _offset += sizes.getInt(_position)
        _position += 1
      }
      override def current(): Object = {
        o.init(ref, _offset, sizes.getInt(_position))
        o
      }
    }
  }
  
   class BinaryColumnFormat(outputOI: ObjectInspector, initialSize: Int)
    extends ColumnFormat[LazyBinary] {
    val arr = new ByteArrayList(initialSize * ColumnarSerDe.getFieldSize(outputOI))

    val sizes = new IntArrayList(initialSize)
    sizes.add(0)

    override def size: Int = sizes.size - 1

    override def append(v: LazyBinary) {
      val w = v.getWritableObject()
      sizes.add(w.getLength())
      arr.addElements(arr.size(), w.getBytes(), 0, w.getLength())
    }

    override def appendNull =
      throw new UnsupportedOperationException("LazyColumnFormat.appendNull()")

    override def build = {
      // Not sure if we should make a copy of the array
      arr.trim
      sizes.trim
      this
    }

    override def iterator: ColumnFormatIterator = new ColumnFormatIterator {
      var _position = 0
      var _offset = 0
      override def nextRow() {
        _offset += sizes.getInt(_position)
        _position += 1
      }
      override def current(): Object = {
        val size = sizes.get(_position)
        val b = new Array[Byte](size)
        arr.getElements(_offset, b, 0, size)
        new BytesWritable(b)
      }
    }
  }
  
  class TimestampColumnFormat(initialSize: Int) extends UncompressedColumnFormat[Timestamp] {
    val arr = new LongArrayList(initialSize)

    override def size: Int = arr.size
    override def append(v: Timestamp) = arr.add(v.getTime())

    override def appendNull = {
      nulls.set(arr.size)
      arr.add(0)
    }

    override def build = {
      arr.trim
      this
    }

    override def iterator: ColumnFormatIterator = new NullBitmapColumnIterator[Timestamp](this) {
      val writable = new TimestampWritable
      override def getObject(i: Int): Object = {
        writable.set(new Timestamp(arr.getLong(i)))
        writable
      }
    }
  }

}
