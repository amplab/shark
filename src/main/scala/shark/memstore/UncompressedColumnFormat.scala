package shark.memstore

import it.unimi.dsi.fastutil.booleans.BooleanArrayList
import it.unimi.dsi.fastutil.bytes.ByteArrayList
import it.unimi.dsi.fastutil.doubles.DoubleArrayList
import it.unimi.dsi.fastutil.floats.FloatArrayList
import it.unimi.dsi.fastutil.ints.IntArrayList
import it.unimi.dsi.fastutil.longs.LongArrayList
import it.unimi.dsi.fastutil.shorts.ShortArrayList

import javaewah.{EWAHCompressedBitmap, IntIterator}

import org.apache.hadoop.hive.serde2.ByteStream
import org.apache.hadoop.hive.serde2.io.{ByteWritable, DoubleWritable, ShortWritable}
import org.apache.hadoop.hive.serde2.`lazy`.{ByteArrayRef, LazyFactory, LazyObject}
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.io.{BooleanWritable, IntWritable, LongWritable, FloatWritable, Text,
  NullWritable}


sealed trait UncompressedColumnFormat[T] extends ColumnFormat[T] {
  val nulls = new EWAHCompressedBitmap
  var nullsIter: IntIterator = _
  var nextNullIndex = -1

  def close() {
    nullsIter = nulls.intIterator
    nextNullIndex = -1
  }
}

object UncompressedColumnFormat {

  class BooleanColumnFormat(initialSize: Int) extends UncompressedColumnFormat[Boolean] {
    val arr = new BooleanArrayList(initialSize)
    val writable = new BooleanWritable

    override def apply(i: Int): Object = {
      while(nullsIter.hasNext && nextNullIndex < i) nextNullIndex = nullsIter.next()
      if (nextNullIndex == i) {
        null
      } else {
        writable.set(arr.getBoolean(i))
        writable
      }
    }

    override def size: Int = arr.size
    override def append(v: Boolean) = arr.add(v)

    override def appendNull = {
      nulls.set(arr.size)
      arr.add(false)
    }

    override def build = {
      super.close
      arr.trim
      this
    }
  }

  class ByteColumnFormat(initialSize: Int) extends UncompressedColumnFormat[Byte] {
    val arr = new ByteArrayList(initialSize)
    val writable = new ByteWritable

    override def apply(i: Int): Object = {
      while(nullsIter.hasNext && nextNullIndex < i) nextNullIndex = nullsIter.next()
      if (nextNullIndex == i) {
        null
      } else {
        writable.set(arr.getByte(i))
        writable
      }
    }

    override def size: Int = arr.size
    override def append(v: Byte) = arr.add(v)

    override def appendNull = {
      nulls.set(arr.size)
      arr.add(0.toByte)
    }

    override def build = {
      super.close
      arr.trim
      this
    }
  }

  class ShortColumnFormat(initialSize: Int) extends UncompressedColumnFormat[Short] {
    val arr = new ShortArrayList(initialSize)
    val writable = new ShortWritable

    override def apply(i: Int): Object = {
      while(nullsIter.hasNext && nextNullIndex < i) nextNullIndex = nullsIter.next()
      if (nextNullIndex == i) {
        null
      } else {
        writable.set(arr.getShort(i))
        writable
      }
    }

    override def size: Int = arr.size
    override def append(v: Short) = arr.add(v)

    override def appendNull = {
      nulls.set(arr.size)
      arr.add(0.toShort)
    }

    override def build = {
      super.close
      arr.trim
      this
    }
  }

  class IntColumnFormat(initialSize: Int) extends UncompressedColumnFormat[Int] {
    val arr = new IntArrayList(initialSize)
    val writable = new IntWritable

    override def apply(i: Int): Object = {
      while(nullsIter.hasNext && nextNullIndex < i) nextNullIndex = nullsIter.next()
      if (nextNullIndex == i) {
        null
      } else {
        writable.set(arr.getInt(i))
        writable
      }
    }

    override def size: Int = arr.size
    override def append(v: Int) = arr.add(v)

    override def appendNull = {
      nulls.set(arr.size)
      arr.add(0)
    }

    override def build = {
      super.close
      arr.trim
      this
    }
  }

  class LongColumnFormat(initialSize: Int) extends UncompressedColumnFormat[Long] {
    val arr = new LongArrayList(initialSize)
    val writable = new LongWritable

    override def apply(i: Int): Object = {
      while(nullsIter.hasNext && nextNullIndex < i) nextNullIndex = nullsIter.next()
      if (nextNullIndex == i) {
        null
      } else {
        writable.set(arr.getLong(i))
        writable
      }
    }

    override def size: Int = arr.size
    override def append(v: Long) = arr.add(v)

    override def appendNull = {
      nulls.set(arr.size)
      arr.add(0)
    }

    override def build = {
      super.close
      arr.trim
      this
    }
  }

  class FloatColumnFormat(initialSize: Int) extends UncompressedColumnFormat[Float] {
    val arr = new FloatArrayList(initialSize)
    val writable = new FloatWritable

    override def apply(i: Int): Object = {
      while(nullsIter.hasNext && nextNullIndex < i) nextNullIndex = nullsIter.next()
      if (nextNullIndex == i) {
        null
      } else {
        writable.set(arr.getFloat(i))
        writable
      }
    }

    override def size: Int = arr.size
    override def append(v: Float) = arr.add(v)

    override def appendNull = {
      nulls.set(arr.size)
      arr.add(0)
    }

    override def build = {
      super.close
      arr.trim
      this
    }
  }

  class DoubleColumnFormat(initialSize: Int) extends UncompressedColumnFormat[Double] {
    val arr = new DoubleArrayList(initialSize)
    val writable = new DoubleWritable

    override def apply(i: Int): Object = {
      while(nullsIter.hasNext && nextNullIndex < i) nextNullIndex = nullsIter.next()
      if (nextNullIndex == i) {
        null
      } else {
        writable.set(arr.getDouble(i))
        writable
      }
    }

    override def size: Int = arr.size
    override def append(v: Double) = arr.add(v)

    override def appendNull = {
      nulls.set(arr.size)
      arr.add(0)
    }

    override def build = {
      super.close
      arr.trim
      this
    }
  }

  class TextColumnFormat(initialSize: Int) extends UncompressedColumnFormat[Text] {
    val arr = new ByteArrayList(initialSize * ColumnarSerDe.STRING_SIZE)
    val writable = new Text
    val starts = new IntArrayList(initialSize)
    starts.add(0)

    override def apply(i: Int): Object = {
      while(nullsIter.hasNext && nextNullIndex < i) nextNullIndex = nullsIter.next()
      if (nextNullIndex == i) {
        null
      } else {
        val start = starts.getInt(i)
        writable.set(arr.elements, start, starts.getInt(i + 1) - start)
        writable
      }
    }

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
      super.close
      arr.trim
      starts.trim
      this
    }
  }

  class VoidColumnFormat extends UncompressedColumnFormat[Void] {
    val void = NullWritable.get()
    private var _size = 0
    override def size: Int = size
    override def apply(i: Int): Object = void
    override def append(v: Void) { _size += 1 }
    override def appendNull() { _size += 1 }
    override def build = this
  }

  class LazyColumnFormat(outputOI: ObjectInspector, initialSize: Int)
    extends UncompressedColumnFormat[ByteStream.Output] {
    val arr = new ByteArrayList(initialSize)
    val o = LazyFactory.createLazyObject(outputOI)
    val ref = new ByteArrayRef()

    // starting position of each serialized object
    val starts = new IntArrayList(initialSize)
    starts.add(0)

    override def apply(i: Int): Object = {
      o.init(ref, starts.getInt(i), starts.getInt(i + 1) - starts.getInt(i))
      o
    }

    override def size: Int = starts.size - 1

    override def append(v: ByteStream.Output) {
      starts.add(arr.size() + v.getCount)
      arr.addElements(arr.size(), v.getData, 0, v.getCount)
    }

    override def appendNull = ()

    override def build = {
      // Not sure if we should make a copy of the array
      arr.trim
      starts.trim
      ref.setData(arr.elements)
      this
    }
  }
}
