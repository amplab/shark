package shark.memstore2.column

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector
import shark.memstore2.column.ColumnStats._
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector
import org.apache.hadoop.io.Text
import java.nio.ByteOrder
import java.nio.ByteBuffer
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ByteObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ShortObjectInspector
import java.sql.Timestamp
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector
import org.apache.hadoop.hive.serde2.`lazy`.LazyBinary
import org.apache.hadoop.io.BytesWritable
import org.apache.hadoop.hive.serde2.ByteStream
import shark.execution.serialization.KryoSerializer


class IntColumnBuilder extends NullableColumnBuilder[Int] {

  _stats = new IntColumnStats()
  
  override def t = INT
  
  override def get(o: Object, oi: ObjectInspector): Int = {
    oi.asInstanceOf[IntObjectInspector].get(o)
  }
  
  override def append(v: Int) = {
    _buffer.putInt(v)
  }
}

class LongColumnBuilder extends NullableColumnBuilder[Long] {

  _stats = new LongColumnStats()
  
  override def t = LONG
  
  override def get(o: Object, oi: ObjectInspector): Long = {
    oi.asInstanceOf[LongObjectInspector].get(o)
  }
  
  override def append(v: Long) = {
    _buffer.putLong(v)
  }
}

class FloatColumnBuilder extends NullableColumnBuilder[Float] {

  _stats = new FloatColumnStats()
  
  override def t = FLOAT
  
  override def get(o: Object, oi: ObjectInspector): Float = {
    oi.asInstanceOf[FloatObjectInspector].get(o)
  }
  
  override def append(v: Float) = {
    _buffer.putFloat(v)
  }
}

class DoubleColumnBuilder extends NullableColumnBuilder[Double] {

  _stats = new DoubleColumnStats()
  
  override def t = DOUBLE
  
  override def get(o: Object, oi: ObjectInspector): Double = {
    oi.asInstanceOf[DoubleObjectInspector].get(o)
  }
  
  override def append(v: Double) = {
    _buffer.putDouble(v)
  }
}

class BooleanColumnBuilder extends NullableColumnBuilder[Boolean] {

  _stats = new BooleanColumnStats()
  
  override def t = BOOLEAN
  
  override def get(o: Object, oi: ObjectInspector): Boolean = {
    oi.asInstanceOf[BooleanObjectInspector].get(o)
  }
  
  override def append(v: Boolean) = {
    _buffer.put(if (v) 1.toByte else 0.toByte)
  }
}

class StringColumnBuilder extends NullableColumnBuilder[Text] {

  _stats = new StringColumnStats()
  
  override def t = STRING
  override def get(o: Object, oi: ObjectInspector): Text = {
    oi.asInstanceOf[StringObjectInspector].getPrimitiveWritableObject(o)
  }
  
  override def append(v: Text) {
    val length = v.getLength()
    _buffer.putInt(length)
    _buffer.put(v.getBytes(), 0, length)
  }
}

class VoidColumnBuilder extends NullableColumnBuilder[Void] {
  
  _stats = new NoOp[Void]()
  override def t = VOID
  override def get(o: Object, oi: ObjectInspector) = null
  override def append(v: Void) = {}

}

class ByteColumnBuilder extends NullableColumnBuilder[Byte] {

  _stats = new ByteColumnStats()
  override def t = BYTE
  override def get(o: Object, oi: ObjectInspector): Byte = {
    oi.asInstanceOf[ByteObjectInspector].get(o)
  }
  
  override def append(v: Byte) = {
    _buffer.put(v)
  }
}

class ShortColumnBuilder extends NullableColumnBuilder[Short] {

  _stats = new ShortColumnStats()
  override def t = SHORT
  override def get(o: Object, oi: ObjectInspector): Short = {
    oi.asInstanceOf[ShortObjectInspector].get(o)
  }
  
  override def append(v: Short) = {
    _buffer.putShort(v)
  }
}

class TimestampColumnBuilder extends NullableColumnBuilder[Timestamp] {

  _stats = new TimestampColumnStats()
  override def t = TIMESTAMP
  override def get(o: Object, oi: ObjectInspector): Timestamp = {
    oi.asInstanceOf[TimestampObjectInspector].getPrimitiveJavaObject(o)
  }
  
  override def append(v: Timestamp) = {
    _buffer.putLong(v.getTime())
    _buffer.putInt(v.getNanos())
  }
}

class BinaryColumnBuilder extends NullableColumnBuilder[BytesWritable] {

  _stats = new NoOp[BytesWritable]()
  override def t = BINARY
  override def get(o: Object, oi: ObjectInspector): BytesWritable = {
    if (o.isInstanceOf[LazyBinary]) {
      o.asInstanceOf[LazyBinary].getWritableObject()
    } else if (o.isInstanceOf[BytesWritable]) {
      o.asInstanceOf[BytesWritable]
    } else {
      throw new UnsupportedOperationException("Unknown binary type " + oi)
    }
  }
  
  override def append(v: BytesWritable) = {
    val length = v.getLength()
    _buffer.putInt(length)
    _buffer.put(v.getBytes(), 0, length)
  }
}

class GenericColumnBuilder(oi: ObjectInspector) extends NullableColumnBuilder[ByteStream.Output] {
  
  _stats = new NoOp[ByteStream.Output]
  private var initialized = false
  override def t = GENERIC

  override def get(o: Object, oi: ObjectInspector) = {
    o.asInstanceOf[ByteStream.Output]
  }
  
  override def append(v: ByteStream.Output) {
    if (!initialized) {
      val objectInspectorSerialized = KryoSerializer.serialize(oi)
      _buffer.putInt(objectInspectorSerialized.size)
      _buffer.put(objectInspectorSerialized)
      initialized = true
    }
    val length = v.getCount()
    _buffer.putInt(length)
    _buffer.put(v.getData(), 0, length)
  }

}
