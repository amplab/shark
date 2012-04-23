package shark

import org.apache.hadoop.io.{BooleanWritable, IntWritable, LongWritable, FloatWritable, Text, NullWritable}
import org.apache.hadoop.hive.serde2.io.ShortWritable
import org.apache.hadoop.hive.serde2.io.DoubleWritable
import org.apache.hadoop.hive.serde2.io.ByteWritable
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category
import org.apache.hadoop.hive.serde2.objectinspector.primitive._
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector
import org.apache.hadoop.hive.serde2.ByteStream
import org.apache.hadoop.hive.serde2.`lazy`.ByteArrayRef
import org.apache.hadoop.hive.serde2.`lazy`.LazyFactory
import it.unimi.dsi.fastutil.booleans.BooleanArrayList
import it.unimi.dsi.fastutil.bytes.ByteArrayList
import it.unimi.dsi.fastutil.ints.IntArrayList
import it.unimi.dsi.fastutil.shorts.ShortArrayList
import it.unimi.dsi.fastutil.longs.LongArrayList
import it.unimi.dsi.fastutil.floats.FloatArrayList
import it.unimi.dsi.fastutil.doubles.DoubleArrayList
import javaewah.EWAHCompressedBitmap
import javaewah.IntIterator

// Columns hold primitive arrays of data
// They provide methods to add objects given their objectInspectors and retrieve writable objects
object Column {
  def apply(oi: ObjectInspector) = {
    oi.getCategory match {
      case Category.PRIMITIVE => {
        oi.asInstanceOf[PrimitiveObjectInspector].getPrimitiveCategory match {
          case PrimitiveCategory.BOOLEAN =>  new BooleanColumn()
          case PrimitiveCategory.BYTE => new ByteColumn()
          case PrimitiveCategory.SHORT => new ShortColumn()
          case PrimitiveCategory.INT => new IntColumn()
          case PrimitiveCategory.LONG => new LongColumn()
          case PrimitiveCategory.FLOAT => new FloatColumn()
          case PrimitiveCategory.DOUBLE => new DoubleColumn()
          case PrimitiveCategory.STRING => new StringColumn()
          case PrimitiveCategory.VOID => new VoidColumn()
          case _ => throw new Exception("Invalid primitive object inspector category")
        }
      }
      case _ => new LazyColumn(oi)
    }
  }
}


abstract class Column {
  val initialSize = 1000000 // Number of rows
  val nulls = new EWAHCompressedBitmap()
  var nullsIter: IntIterator = null
  var nextNullIndex = -1

  def apply(i: Int): Object

  def add(o: Object, oi: ObjectInspector)

  def close(): Unit 
}


class BooleanColumn extends Column {

  val arr = new BooleanArrayList(initialSize)
  val w = new BooleanWritable()

  override def apply(i: Int) = {
    while(nullsIter.hasNext && nextNullIndex < i) nextNullIndex = nullsIter.next() 
    if (nextNullIndex == i) {
      null
    } else {
      w.set(arr.getBoolean(i))
      w
    }
  }

  override def add(o: Object, oi: ObjectInspector) {
    if (o == null) {
      nulls.set(arr.size)
      arr.add(false)
    } else {
      arr.add(oi.asInstanceOf[BooleanObjectInspector].get(o))
    }
  }

  override def close() {
    nullsIter = nulls.intIterator
    nextNullIndex = -1
    arr.trim()
  }  
}


class ByteColumn extends Column {

  val arr = new ByteArrayList(initialSize)
  val w = new ByteWritable()

  override def apply(i: Int) = {
    while(nullsIter.hasNext && nextNullIndex < i) nextNullIndex = nullsIter.next() 
    if (nextNullIndex == i) {
      null
    } else {
      w.set(arr.getByte(i))
      w
    }
  }

  override def add(o: Object, oi: ObjectInspector) {
    if (o == null) {
      nulls.set(arr.size)
      arr.add(0.toByte)
    } else {
      arr.add(oi.asInstanceOf[ByteObjectInspector].get(o))
    }
  }

  override def close() {
    nextNullIndex = -1
    nullsIter = nulls.intIterator
    arr.trim()
  }  
}


class ShortColumn extends Column {

  val arr = new ShortArrayList(initialSize)
  val w = new ShortWritable()

  override def apply(i: Int) = {
    while(nullsIter.hasNext && nextNullIndex < i) nextNullIndex = nullsIter.next() 
    if (nextNullIndex == i) {
      null
    } else {
      w.set(arr.getShort(i))
      w
    }
  }

  override def add(o: Object, oi: ObjectInspector) {
    if (o == null) {
      nulls.set(arr.size)
      arr.add(0.toShort)
    } else {
      arr.add(oi.asInstanceOf[ShortObjectInspector].get(o))
    }
  }

  override def close() {
    nextNullIndex = -1
    nullsIter = nulls.intIterator
    arr.trim()
  }  
}


class IntColumn extends Column {

  val arr = new IntArrayList(initialSize)
  val w = new IntWritable()

  override def apply(i: Int) = {
    while(nullsIter.hasNext && nextNullIndex < i) nextNullIndex = nullsIter.next() 
    if (nextNullIndex == i) {
      null
    } else {
      w.set(arr.getInt(i))
      w
    }
  }

  override def add(o: Object, oi: ObjectInspector) {
    if (o == null) {
      nulls.set(arr.size) 
      arr.add(0)
    } else {
      arr.add(oi.asInstanceOf[IntObjectInspector].get(o))
    }
  }

  override def close() {
    nextNullIndex = -1
    nullsIter = nulls.intIterator
    arr.trim()
  }  
}


class LongColumn extends Column {

  val arr = new LongArrayList(initialSize)
  val w = new LongWritable()

  override def apply(i: Int) = {
    while(nullsIter.hasNext && nextNullIndex < i) nextNullIndex = nullsIter.next() 
    if (nextNullIndex == i) {
      null
    } else {
      w.set(arr.getLong(i))
      w
    }
  }

  override def add(o: Object, oi: ObjectInspector) {
    if (o == null) {
      nulls.set(arr.size)
      arr.add(0)
    } else {
      arr.add(oi.asInstanceOf[LongObjectInspector].get(o))
    }
  }

  override def close() {
    nextNullIndex = -1
    nullsIter = nulls.intIterator
    arr.trim()
  }  
}


class FloatColumn extends Column {

  val arr = new FloatArrayList(initialSize)
  val w = new FloatWritable()

  override def apply(i: Int) = {
    while(nullsIter.hasNext && nextNullIndex < i) nextNullIndex = nullsIter.next() 
    if (nextNullIndex == i) {
      null
    } else {
      w.set(arr.getFloat(i))
      w
    }
  }

  override def add(o: Object, oi: ObjectInspector) {
    if (o == null) {
      nulls.set(arr.size)
      arr.add(0)
    } else {
      arr.add(oi.asInstanceOf[FloatObjectInspector].get(o))
    }
  }

  override def close() {
    nextNullIndex = -1
    nullsIter = nulls.intIterator
    arr.trim()
  }  
}


class DoubleColumn extends Column {

  val arr = new DoubleArrayList(initialSize)
  val w = new DoubleWritable()

  override def apply(i: Int) = {
    while(nullsIter.hasNext && nextNullIndex < i) nextNullIndex = nullsIter.next() 
    if (nextNullIndex == i) {
      null
    } else {
      w.set(arr.getDouble(i))
      w
    }
  }

  override def add(o: Object, oi: ObjectInspector) {
    if (o == null) {
      nulls.set(arr.size)
      arr.add(0)
    } else {
      arr.add(oi.asInstanceOf[DoubleObjectInspector].get(o))
    }
  }

  override def close() {
    nextNullIndex = -1
    nullsIter = nulls.intIterator
    arr.trim()
  }  
}


class StringColumn extends Column {

  val arr = new ByteArrayList(initialSize)
  val starts = new IntArrayList(initialSize) // start of each string
  val w: Text = new Text()

  starts.add(0)

  override def apply(i: Int) = {
    while(nullsIter.hasNext && nextNullIndex < i) nextNullIndex = nullsIter.next() 
    if (nextNullIndex == i) {
      null
    } else {
      val start = starts.getInt(i)
      w.set(arr.elements, start, starts.getInt(i + 1) - start)
      w
    }
  }

  override def add(o: Object, oi: ObjectInspector) {
    if (o == null) {
      nulls.set(starts.size - 1)
      starts.add(arr.size)
    } else {
      val text = oi.asInstanceOf[StringObjectInspector].getPrimitiveWritableObject(o)
      starts.add(arr.size() + text.getLength)
      arr.addElements(arr.size(), text.getBytes, 0, text.getLength)
    }
  }

  override def close() {
    nextNullIndex = -1
    nullsIter = nulls.intIterator
    arr.trim()
    starts.trim()
  }
}


class VoidColumn extends Column {
  val void = NullWritable.get()
  override def apply(i: Int) = void
  override def add(o: Object, oi: ObjectInspector) {}
  override def close() {}
}


// Holds non-primitive column in serialized form
class LazyColumn(outputOI: ObjectInspector) extends Column {

  val arr = new ByteArrayList(initialSize) // all serialized bytes
  val starts = new IntArrayList(initialSize) // start of each serialized object
  val o = LazyFactory.createLazyObject(outputOI)
  val ref = new ByteArrayRef()

  starts.add(0)

  override def apply(i: Int) = {
    val start = starts.getInt(i)
    o.init(ref, start, starts.getInt(i + 1) - start)
    o
  }

  override def add(o: Object, oi: ObjectInspector) { //Our object is an already serialized bytearray
    val bytes = o.asInstanceOf[ByteStream.Output]
    starts.add(arr.size() + bytes.getCount)
    arr.addElements(arr.size(), bytes.getData, 0, bytes.getCount)
  }

  // Not sure if we should make a copy of the array
  override def close() {
    arr.trim()
    starts.trim()
    ref.setData(arr.elements)
  }
}
