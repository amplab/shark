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

  class TextColumnFormat(initialSize: Int) extends UncompressedColumnFormat[Text] {
    val arr = new ByteArrayList(initialSize * ColumnarSerDe.STRING_SIZE)
    val starts = new IntArrayList(initialSize)
    starts.add(0)

    override def size: Int = starts.size - 1

    override def append(v: Text) = {
      starts.add(arr.size + v.getLength)
      arr.addElements(arr.size, v.getBytes, 0, v.getLength)
    }

    override def appendNull() {
      nulls.set(starts.size - 1)
      starts.add(arr.size)
    }

    override def build = {
      arr.trim
      starts.trim
      this
    }

    override def iterator: ColumnFormatIterator = new NullBitmapColumnIterator[Text](this) {
      val writable = new Text
      override def getObject(i: Int): Object = {
        val start = starts.getInt(i)
        writable.set(arr.elements, start, starts.getInt(i + 1) - start)
        writable
      }
    }
  }

  class VoidColumnFormat extends UncompressedColumnFormat[Void] {
    private var _size = 0
    override def size: Int = size
    override def append(v: Void) { _size += 1 }
    override def appendNull() { _size += 1 }
    override def build = this
    override def iterator: ColumnFormatIterator = new NullBitmapColumnIterator[Void](this) {
      val void = NullWritable.get()
      override def getObject(i: Int): Object = void
    }
  }

  class LazyColumnFormat(outputOI: ObjectInspector, initialSize: Int)
    extends UncompressedColumnFormat[ByteStream.Output] {
    val arr = new ByteArrayList(initialSize * ColumnarSerDe.getFieldSize(outputOI))
    val ref = new ByteArrayRef()

    // starting position of each serialized object
    val starts = new IntArrayList(initialSize)
    starts.add(0)

    override def size: Int = starts.size - 1

    override def append(v: ByteStream.Output) {
      starts.add(arr.size() + v.getCount)
      arr.addElements(arr.size(), v.getData, 0, v.getCount)
    }

    override def appendNull =
      throw new UnsupportedOperationException("LazyColumnFormat.appendNull()")

    override def build = {
      // Not sure if we should make a copy of the array
      arr.trim
      starts.trim
      ref.setData(arr.elements)
      this
    }

    override def iterator: ColumnFormatIterator = {
      new NullBitmapColumnIterator[ByteStream.Output](this) {
        val o = LazyFactory.createLazyObject(outputOI)
        override def getObject(i: Int): Object = {
          o.init(ref, starts.getInt(i), starts.getInt(i + 1) - starts.getInt(i))
          o
        }
      }
    }
  }
}
