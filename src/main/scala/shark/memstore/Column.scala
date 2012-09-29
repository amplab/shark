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
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector, PrimitiveObjectInspector}
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory
import org.apache.hadoop.hive.serde2.objectinspector.primitive.{BooleanObjectInspector,
  ByteObjectInspector, ShortObjectInspector, IntObjectInspector, LongObjectInspector,
  FloatObjectInspector, DoubleObjectInspector, StringObjectInspector}
import org.apache.hadoop.io.{BooleanWritable, FloatWritable, IntWritable, LongWritable,
  NullWritable, Text}


/**
 * A immutable column of data, backed by some primitive array(s). These columns
 * provide methods to add objects using their object inspectors and return
 * Hadoop writable objects. A column object should be built by a ColumnBuilder.
 */
trait Column {
  def apply(i: Int): Object
  def size: Int
}

/**
 * An append-only, mutable data structure used to build a Column.
 */
trait ColumnBuilder {
  def add(o: Object, oi: ObjectInspector)
  def build: Column
}


trait ColumnBuilderFactory {
  def createBuilder(oi: ObjectInspector, initialSize: Int): ColumnBuilder
}


object Column extends ColumnBuilderFactory {
  override def createBuilder(oi: ObjectInspector, initialSize: Int): ColumnBuilder = {
    oi.getCategory match {
      case ObjectInspector.Category.PRIMITIVE => {
        oi.asInstanceOf[PrimitiveObjectInspector].getPrimitiveCategory match {
          case PrimitiveCategory.BOOLEAN => new BooleanColumn.Builder(initialSize)
          case PrimitiveCategory.BYTE => new ByteColumn.Builder(initialSize)
          case PrimitiveCategory.SHORT => new ShortColumn.Builder(initialSize)
          case PrimitiveCategory.INT => new IntColumn.Builder(initialSize)
          case PrimitiveCategory.LONG => new LongColumn.Builder(initialSize)
          case PrimitiveCategory.FLOAT => new FloatColumn.Builder(initialSize)
          case PrimitiveCategory.DOUBLE => new DoubleColumn.Builder(initialSize)
          case PrimitiveCategory.STRING => new StringColumn.Builder(initialSize)
          case PrimitiveCategory.VOID => new VoidColumn.Builder()
          case _ => throw new Exception("Invalid primitive object inspector category")
        }
      }
      case _ => new LazyColumn.Builder(oi, initialSize)
    }
  }

  class BooleanColumn private (val arr: Array[Boolean], val nulls: EWAHCompressedBitmap)
  extends Column {
    var nullsIter: IntIterator = nulls.intIterator
    var nextNullIndex = -1
    val w = new BooleanWritable

    override def apply(i: Int) = {
      while(nullsIter.hasNext && nextNullIndex < i) nextNullIndex = nullsIter.next()
      if (nextNullIndex == i) {
        null
      } else {
        w.set(arr(i))
        w
      }
    }

    override def size: Int = arr.size
  }

  object BooleanColumn {
    class Builder(initialSize: Int) extends ColumnBuilder {
      val arr = new BooleanArrayList(initialSize)
      val nulls = new EWAHCompressedBitmap

      override def add(o: Object, oi: ObjectInspector) {
        if (o == null) {
          nulls.set(arr.size)
          arr.add(false)
        } else {
          arr.add(oi.asInstanceOf[BooleanObjectInspector].get(o))
        }
      }

      override def build: Column = {
        arr.trim()
        new BooleanColumn(arr.elements, nulls)
      }
    }
  }

  class ByteColumn private (val arr: Array[Byte], val nulls: EWAHCompressedBitmap) extends Column {
    var nullsIter: IntIterator = nulls.intIterator
    var nextNullIndex = -1
    val w = new ByteWritable

    override def apply(i: Int) = {
      while(nullsIter.hasNext && nextNullIndex < i) nextNullIndex = nullsIter.next()
      if (nextNullIndex == i) {
        null
      } else {
        w.set(arr(i))
        w
      }
    }

    override def size: Int = arr.size
  }

  object ByteColumn {
    class Builder(initialSize: Int) extends ColumnBuilder {
      val arr = new ByteArrayList(initialSize)
      val nulls = new EWAHCompressedBitmap

      override def add(o: Object, oi: ObjectInspector) {
        if (o == null) {
          nulls.set(arr.size)
          arr.add(0.toByte)
        } else {
          arr.add(oi.asInstanceOf[ByteObjectInspector].get(o))
        }
      }

      override def build: Column = {
        arr.trim()
        new ByteColumn(arr.elements, nulls)
      }
    }
  }

  class ShortColumn private (val arr: Array[Short], val nulls: EWAHCompressedBitmap)
  extends Column {
    var nullsIter: IntIterator = nulls.intIterator
    var nextNullIndex = -1
    val w = new ShortWritable

    override def apply(i: Int) = {
      while(nullsIter.hasNext && nextNullIndex < i) nextNullIndex = nullsIter.next()
      if (nextNullIndex == i) {
        null
      } else {
        w.set(arr(i))
        w
      }
    }

    override def size: Int = arr.size
  }

  object ShortColumn {
    class Builder(initialSize: Int) extends ColumnBuilder {
      val arr = new ShortArrayList(initialSize)
      val nulls = new EWAHCompressedBitmap

      override def add(o: Object, oi: ObjectInspector) {
        if (o == null) {
          nulls.set(arr.size)
          arr.add(0.toByte)
        } else {
          arr.add(oi.asInstanceOf[ShortObjectInspector].get(o))
        }
      }

      override def build: Column = {
        arr.trim()
        new ShortColumn(arr.elements, nulls)
      }
    }
  }

  class IntColumn private (val arr: Array[Int], val nulls: EWAHCompressedBitmap) extends Column {
    var nullsIter: IntIterator = nulls.intIterator
    var nextNullIndex = -1
    val w = new IntWritable

    override def apply(i: Int) = {
      while(nullsIter.hasNext && nextNullIndex < i) nextNullIndex = nullsIter.next()
      if (nextNullIndex == i) {
        null
      } else {
        w.set(arr(i))
        w
      }
    }

    override def size: Int = arr.size
  }

  object IntColumn {
    class Builder(initialSize: Int) extends ColumnBuilder {
      val arr = new IntArrayList(initialSize)
      val nulls = new EWAHCompressedBitmap

      override def add(o: Object, oi: ObjectInspector) {
        if (o == null) {
          nulls.set(arr.size)
          arr.add(0.toByte)
        } else {
          arr.add(oi.asInstanceOf[IntObjectInspector].get(o))
        }
      }

      override def build: Column = {
        arr.trim()
        new IntColumn(arr.elements, nulls)
      }
    }
  }

  class LongColumn private (val arr: Array[Long], val nulls: EWAHCompressedBitmap) extends Column {
    var nullsIter: IntIterator = nulls.intIterator
    var nextNullIndex = -1
    val w = new LongWritable

    override def apply(i: Int) = {
      while(nullsIter.hasNext && nextNullIndex < i) nextNullIndex = nullsIter.next()
      if (nextNullIndex == i) {
        null
      } else {
        w.set(arr(i))
        w
      }
    }

    override def size: Int = arr.size
  }

  object LongColumn {
    class Builder(initialSize: Int) extends ColumnBuilder {
      val arr = new LongArrayList(initialSize)
      val nulls = new EWAHCompressedBitmap

      override def add(o: Object, oi: ObjectInspector) {
        if (o == null) {
          nulls.set(arr.size)
          arr.add(0.toByte)
        } else {
          arr.add(oi.asInstanceOf[LongObjectInspector].get(o))
        }
      }

      override def build: Column = {
        arr.trim()
        new LongColumn(arr.elements, nulls)
      }
    }
  }

  class FloatColumn private (val arr: Array[Float], val nulls: EWAHCompressedBitmap)
  extends Column {
    var nullsIter: IntIterator = nulls.intIterator
    var nextNullIndex = -1
    val w = new FloatWritable

    override def apply(i: Int) = {
      while(nullsIter.hasNext && nextNullIndex < i) nextNullIndex = nullsIter.next()
      if (nextNullIndex == i) {
        null
      } else {
        w.set(arr(i))
        w
      }
    }

    override def size: Int = arr.size
  }

  object FloatColumn {
    class Builder(initialSize: Int) extends ColumnBuilder {
      val arr = new FloatArrayList(initialSize)
      val nulls = new EWAHCompressedBitmap

      override def add(o: Object, oi: ObjectInspector) {
        if (o == null) {
          nulls.set(arr.size)
          arr.add(0.toByte)
        } else {
          arr.add(oi.asInstanceOf[FloatObjectInspector].get(o))
        }
      }

      override def build: Column = {
        arr.trim()
        new FloatColumn(arr.elements, nulls)
      }
    }
  }

  class DoubleColumn private (val arr: Array[Double], val nulls: EWAHCompressedBitmap)
  extends Column {
    var nullsIter: IntIterator = nulls.intIterator
    var nextNullIndex = -1
    val w = new DoubleWritable

    override def apply(i: Int) = {
      while(nullsIter.hasNext && nextNullIndex < i) nextNullIndex = nullsIter.next()
      if (nextNullIndex == i) {
        null
      } else {
        w.set(arr(i))
        w
      }
    }

    override def size: Int = arr.size
  }

  object DoubleColumn {
    class Builder(initialSize: Int) extends ColumnBuilder {
      val arr = new DoubleArrayList(initialSize)
      val nulls = new EWAHCompressedBitmap

      override def add(o: Object, oi: ObjectInspector) {
        if (o == null) {
          nulls.set(arr.size)
          arr.add(0.toByte)
        } else {
          arr.add(oi.asInstanceOf[DoubleObjectInspector].get(o))
        }
      }

      override def build: Column = {
        arr.trim()
        new DoubleColumn(arr.elements, nulls)
      }
    }
  }

  /**
   * A string column. Store a collection of strings using a single byte array by
   * concatenating them together. An additional int array is used to index the
   * starting position of each string.
   */
  class StringColumn private (arr: Array[Byte], starts: Array[Int], val nulls: EWAHCompressedBitmap)
  extends Column {
    var nullsIter: IntIterator = nulls.intIterator
    var nextNullIndex = -1
    val w = new Text

    override def apply(i: Int) = {
      while(nullsIter.hasNext && nextNullIndex < i) nextNullIndex = nullsIter.next()
      if (nextNullIndex == i) {
        null
      } else {
        w.set(arr, starts(i), starts(i + 1) - starts(i))
        w
      }
    }

    override def size: Int = starts.size - 1
  }

  object StringColumn {
    class Builder(initialSize: Int) extends ColumnBuilder {
      val arr = new ByteArrayList(initialSize * ColumnarSerDe.STRING_SIZE)
      val nulls = new EWAHCompressedBitmap
      val starts = new IntArrayList(initialSize)
      starts.add(0)

      override def add(o: Object, oi: ObjectInspector) {
        if (o == null) {
          nulls.set(starts.size - 1)
          starts.add(arr.size)
        } else {
          val text: Text = oi.asInstanceOf[StringObjectInspector].getPrimitiveWritableObject(o)
          starts.add(arr.size() + text.getLength)
          arr.addElements(arr.size, text.getBytes, 0, text.getLength)
        }
      }

      override def build: Column = {
        arr.trim()
        starts.trim()
        new StringColumn(arr.elements, starts.elements, nulls)
      }
    }
  }

  class VoidColumn private (val size: Int) extends Column {
    val void = NullWritable.get()
    override def apply(i: Int) = void
  }

  object VoidColumn {
    class Builder extends ColumnBuilder {
      private var _size = 0
      override def add(o: Object, oi: ObjectInspector) = _size += 1
      override def build: Column = new VoidColumn(_size)
    }
  }

  /**
   * For non-primitive columns, serialize the value and store them as a single
   * byte array.
   */
  class LazyColumn private (
    arr: Array[Byte], 
    starts: Array[Int],
    ref: ByteArrayRef,
    o: LazyObject[_])
  extends Column {

    override def apply(i: Int) = {
      o.init(ref, starts(i), starts(i + 1) - starts(i))
      o
    }

    override def size: Int = starts.size - 1
  }

  object LazyColumn {
    class Builder(outputOI: ObjectInspector, initialSize: Int) extends ColumnBuilder {
      // Multiply the initialSize by a factor that's based on the object types
      // contained by the non-primitive object.
      val arr = new ByteArrayList(initialSize * ColumnarSerDe.getFieldSize(outputOI))
      // Start of each serialized object.
      val starts = new IntArrayList(initialSize)
      starts.add(0)
      val ref = new ByteArrayRef()
      val o = LazyFactory.createLazyObject(outputOI)

      // The object is already a serialized bytearray.
      override def add(o: Object, oi: ObjectInspector) {
        val bytes = o.asInstanceOf[ByteStream.Output]
        starts.add(arr.size() + bytes.getCount)
        arr.addElements(arr.size(), bytes.getData, 0, bytes.getCount)
      }

      // Not sure if we should make a copy of the array
      override def build: Column = {
        arr.trim()
        starts.trim()
        ref.setData(arr.elements)
        new LazyColumn(arr.elements, starts.elements, ref, o)
      }
    }
  }
}
