package shark.memstore2.column

import java.nio.ByteBuffer
import java.sql.Timestamp
import org.apache.hadoop.hive.serde2.io.ByteWritable
import org.apache.hadoop.hive.serde2.io.DoubleWritable
import org.apache.hadoop.hive.serde2.io.ShortWritable
import org.apache.hadoop.hive.serde2.io.TimestampWritable
import org.apache.hadoop.io.BooleanWritable
import org.apache.hadoop.io.BytesWritable
import org.apache.hadoop.io.FloatWritable
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.hive.serde2.`lazy`.LazyObject
import shark.execution.serialization.KryoSerializer
import org.apache.hadoop.hive.serde2.`lazy`.LazyFactory
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.hive.serde2.`lazy`.ByteArrayRef

class IntColumnIterator(val buffer: ByteBuffer) extends ColumnIterator {
  private val _writable = new IntWritable

  override def next() {
    _writable.set(buffer.getInt())
  }

  override def current() = _writable
}

class FloatColumnIterator(val buffer: ByteBuffer) extends ColumnIterator {
  private val _writable = new FloatWritable

  override def next() {
    _writable.set(buffer.getFloat())
  }

  override def current = _writable
}

class LongColumnIterator(val buffer: ByteBuffer) extends ColumnIterator {
  private val _writable = new LongWritable

  override def next() {
    _writable.set(buffer.getLong())
  }

  override def current = _writable
}

class DoubleColumnIterator(val buffer: ByteBuffer) extends ColumnIterator {
  private val _writable = new DoubleWritable

  override def next() {
    _writable.set(buffer.getDouble())
  }

  override def current = _writable
}

class BooleanColumnIterator(val buffer: ByteBuffer) extends ColumnIterator {
  private val _writable = new BooleanWritable

  override def next() {
    _writable.set(buffer.get == 1)
  }

  override def current = _writable
}

class ByteColumnIterator(val buffer: ByteBuffer) extends ColumnIterator {
  private val _writable = new ByteWritable

  override def next() {
    _writable.set(buffer.get)
  }

  override def current = _writable
}

class ShortColumnIterator(val buffer: ByteBuffer) extends ColumnIterator {
  private val _writable = new ShortWritable

  override def next() {
    _writable.set(buffer.getShort())
  }

  override def current = _writable
}

class VoidColumnIterator(val buffer: ByteBuffer) extends ColumnIterator {
  private val _writable = NullWritable.get()
  override def next() {}
  override def current = _writable
}

class TimestampColumnIterator(val buffer: ByteBuffer) extends ColumnIterator {
  private val _timestamp = new Timestamp(0)
  private val _writable = new TimestampWritable(_timestamp)

  override def next() {
    _timestamp.setTime(buffer.getLong())
    _timestamp.setNanos(buffer.getInt())
  }

  override def current = _writable
}

class BinaryColumnIterator(val buffer: ByteBuffer) extends ColumnIterator {
  private val _writable = new BytesWritable
  private var _currentLen = 0
  private val _bytesFld = {
    val f = _writable.getClass().getDeclaredField("bytes")
    f.setAccessible(true)
    f
  }
  private val _lengthFld = {
    val f = _writable.getClass().getDeclaredField("size")
    f.setAccessible(true)
    f
  }
  override def next() {
    // Ideally we want to set the bytes in BytesWritable directly without creating
    // a new byte array.
    val _currentLen = buffer.getInt
    if (_currentLen >= 0) {
      val newBytes = new Array[Byte](_currentLen)
      buffer.get(newBytes, 0, _currentLen)
      _bytesFld.set(_writable, newBytes)
      _lengthFld.set(_writable, _currentLen)
    }
  }

  override def current = if (_currentLen >= 0) _writable else null
}

class StringColumnIterator(val buffer: ByteBuffer) extends ColumnIterator {
  private val _writable = new Text
  private var _currentLen = 0
  private val _bytesFld = {
    val f = _writable.getClass().getDeclaredField("bytes")
    f.setAccessible(true)
    f
  }
  private val _lengthFld = {
    val f = _writable.getClass().getDeclaredField("length")
    f.setAccessible(true)
    f
  }

  override def next() {
    _currentLen = buffer.getInt
    if (_currentLen >= 0) {
      var b = _bytesFld.get(_writable).asInstanceOf[Array[Byte]]
      if (b == null || b.length < _currentLen) {
        b = new Array[Byte](_currentLen)
        _bytesFld.set(_writable, b)
      }
      buffer.get(b, 0, _currentLen)
      _lengthFld.set(_writable, _currentLen)
    }
  }

  override def current: Text = if (_currentLen >= 0) _writable else null
}

class GenericColumnIterator(val buffer: ByteBuffer) extends ColumnIterator {

  private var _initialized = false
  private var _obj: LazyObject[_] = _

  private val _byteArrayRef = new ByteArrayRef()
  override def next() = {
    //the very first record actually represents the serialized OI
    if (!_initialized) {
      val oiSize = buffer.getInt()
      val oiSerialized = new Array[Byte](oiSize)
      buffer.get(oiSerialized, 0, oiSize)
      val oi = KryoSerializer.deserialize[ObjectInspector](oiSerialized)
      _obj = LazyFactory.createLazyObject(oi)
      _initialized = true
    }
    val len = buffer.getInt
    val bytes = new Array[Byte](len)
    buffer.get(bytes, 0, len)
    _byteArrayRef.setData(bytes)
    _obj.init(_byteArrayRef, 0, len)
  }

  override def current = _obj
}