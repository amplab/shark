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

import java.nio.ByteBuffer
import java.nio.ByteOrder
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory
import org.apache.hadoop.io.BytesWritable
import java.sql.Timestamp
import org.apache.hadoop.io.Text
import org.apache.hadoop.hive.serde2.ByteStream
import org.apache.hadoop.hive.serde2.objectinspector.primitive._
import org.apache.hadoop.hive.serde2.`lazy`.LazyBinary
import shark.execution.serialization.KryoSerializer
import shark.memstore2.column._
import shark.memstore2.column.ColumnStats._
import org.apache.hadoop.io.Writable
import org.apache.hadoop.hive.serde2.io.TimestampWritable
import org.apache.hadoop.hive.serde2.`lazy`.LazyObject
import org.apache.hadoop.hive.serde2.`lazy`.ByteArrayRef
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.FloatWritable
import org.apache.hadoop.io.BooleanWritable
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.hive.serde2.io.ByteWritable
import org.apache.hadoop.hive.serde2.io.DoubleWritable
import org.apache.hadoop.hive.serde2.io.ShortWritable



trait ColumnBuilder[T] {

  def t: ColumnType[T, _]
  private var _buffer: ByteBuffer = _
  private var _initialSize: Int = _
  def stats: ColumnStats[T]

  def append(o: Object, oi: ObjectInspector): Unit = {
    _buffer = growIfNeeded(_buffer)
    val v = t.get(o, oi)
    
    t.append(v, _buffer)
    gatherStats(v)
  }

  protected def gatherStats(v: T) {
    stats.append(v)
  }

  def build: ByteBuffer = {
    _buffer.limit(_buffer.position())
    _buffer.rewind()
    _buffer
  }

  /**
   * Initialize with an approximate lower bound on the expected number
   * of elements in this column.
   */
  def initialize(numRows: Int):ByteBuffer = {
    _initialSize = if(numRows == 0) 1024*1024*10 else numRows*t.size
    _buffer = ByteBuffer.allocate(_initialSize + 4 + 4)
    _buffer.order(ByteOrder.nativeOrder())
    _buffer.putInt(t.typeID)
  }
  
  protected def growIfNeeded(orig: ByteBuffer, loadFactor: Double = 0.75): ByteBuffer = {
    val capacity = orig.capacity()
    if (orig.remaining() < (1 - loadFactor) * capacity) {
      //grow in steps of initial size 
      val s = orig.capacity() + _initialSize/2
      val pos = orig.position()
      orig.clear()
      val b = ByteBuffer.allocate(s)
      b.order(ByteOrder.nativeOrder())
      b.put(orig.array(), 0, pos)
    } else {
      orig
    }
  }

}

class DefaultColumnBuilder[T](val stats: ColumnStats[T], val t: ColumnType[T, _]) 
  extends CompressedColumnBuilder[T] with NullableColumnBuilder[T]{}


trait CompressedColumnBuilder[T] extends ColumnBuilder[T] {

  var compressionSchemes:Seq[CompressionAlgorithm] = _
  private var _default = new NoCompression()

  override def initialize(numRows: Int) = {
    val v = super.initialize(numRows)
    compressionSchemes = Seq(new RLE())
    v
  }

  def shouldApply(scheme: CompressionAlgorithm): Boolean = {
    scheme.compressibilityScore < 0.8
  }

  override protected def gatherStats(v: T) = {
    compressionSchemes.foreach { scheme =>
      if (scheme.supportsType(t)) {
        scheme.gatherStatsForCompressability(v, t)
      }
    }
    super.gatherStats(v)
  }

  override def build = {
    val b = super.build
    
    if (compressionSchemes.isEmpty) {
      _default.compress(b, t)
    } else {
      val candidateScheme = compressionSchemes.minBy(_.compressibilityScore)
      if (shouldApply(candidateScheme)) {
        candidateScheme.compress(b, t)
      } else {
        _default.compress(b, t)
      }
    }
  }
}

abstract class ColumnType[T, V](val typeID: Int, val size: Int) {
  def extract(currentPos: Int, buffer: ByteBuffer): T
  def append(v: T, buffer: ByteBuffer)
  def get(o: Object, oi: ObjectInspector): T
  def actualSize(v: T) = size
  
  def extractInto(currentPos: Int, buffer: ByteBuffer, writable: V)
  def newWritable(): V
  def clone(v: T): T = v
}

object INT extends ColumnType[Int, IntWritable](0, 4) {

  override def append(v: Int, buffer: ByteBuffer) = {
    buffer.putInt(v)
  }

  override def extract(currentPos: Int, buffer: ByteBuffer) = {
    buffer.getInt()
  }

  override def get(o: Object, oi: ObjectInspector): Int = {
    oi.asInstanceOf[IntObjectInspector].get(o)
  }

  override def extractInto(currentPos: Int, buffer: ByteBuffer, writable: IntWritable) = {
    writable.set(extract(currentPos, buffer))
  }

  override def newWritable() = new IntWritable
}

object LONG extends ColumnType[Long, LongWritable](1, 8) {

  override def append(v: Long, buffer: ByteBuffer) = {
    buffer.putLong(v)
  }

  override def extract(currentPos: Int, buffer: ByteBuffer) = {
    buffer.getLong()
  }

  override def get(o: Object, oi: ObjectInspector): Long = {
    oi.asInstanceOf[LongObjectInspector].get(o)
  }
  
  def extractInto(currentPos: Int, buffer: ByteBuffer, writable: LongWritable) = {
    writable.set(extract(currentPos, buffer))
  }

  def newWritable() = new LongWritable
  
}

object FLOAT extends ColumnType[Float, FloatWritable](2, 4) {
  override def append(v: Float, buffer: ByteBuffer) = {
    buffer.putFloat(v)
  }

  override def extract(currentPos: Int, buffer: ByteBuffer) = {
    buffer.getFloat()
  }

  override def get(o: Object, oi: ObjectInspector): Float = {
    oi.asInstanceOf[FloatObjectInspector].get(o)
  }

  def extractInto(currentPos: Int, buffer: ByteBuffer, writable: FloatWritable) = {
    writable.set(extract(currentPos, buffer))
  }

  def newWritable() = new FloatWritable
}

object DOUBLE extends ColumnType[Double, DoubleWritable](3, 8) {

  override def append(v: Double, buffer: ByteBuffer) = {
    buffer.putDouble(v)
  }

  override def extract(currentPos: Int, buffer: ByteBuffer) = {
    buffer.getDouble()
  }
  override def get(o: Object, oi: ObjectInspector): Double = {
    oi.asInstanceOf[DoubleObjectInspector].get(o)
  }

  def extractInto(currentPos: Int, buffer: ByteBuffer, writable: DoubleWritable) = {
    writable.set(extract(currentPos, buffer))
  }

  def newWritable() = new DoubleWritable
}

object BOOLEAN extends ColumnType[Boolean, BooleanWritable](4, 1) {
  override def append(v: Boolean, buffer: ByteBuffer) = {
    buffer.put(if (v) 1.toByte else 0.toByte)
  }

  override def extract(currentPos: Int, buffer: ByteBuffer) = {
    if (buffer.get() == 1) true else false
  }

  override def get(o: Object, oi: ObjectInspector): Boolean = {
    oi.asInstanceOf[BooleanObjectInspector].get(o)
  }

  def extractInto(currentPos: Int, buffer: ByteBuffer, writable: BooleanWritable) = {
    writable.set(extract(currentPos, buffer))
  }

  def newWritable() = new BooleanWritable
}

object BYTE extends ColumnType[Byte, ByteWritable](5, 1) {

  override def append(v: Byte, buffer: ByteBuffer) = {
    buffer.put(v)
  }
  
  override def extract(currentPos: Int, buffer: ByteBuffer) = {
    buffer.get()
  }
  override def get(o: Object, oi: ObjectInspector): Byte = {
    oi.asInstanceOf[ByteObjectInspector].get(o)
  }

  def extractInto(currentPos: Int, buffer: ByteBuffer, writable: ByteWritable) = {
    writable.set(extract(currentPos, buffer))
  }

  def newWritable() = new ByteWritable
}

object SHORT extends ColumnType[Short, ShortWritable](6, 2) {

  override def append(v: Short, buffer: ByteBuffer) = {
    buffer.putShort(v)
  }
  
  override def extract(currentPos: Int, buffer: ByteBuffer) = {
    buffer.getShort()
  }

  override def get(o: Object, oi: ObjectInspector): Short = {
    oi.asInstanceOf[ShortObjectInspector].get(o)
  }

  def extractInto(currentPos: Int, buffer: ByteBuffer, writable: ShortWritable) = {
    writable.set(extract(currentPos, buffer))
  }

  def newWritable() = new ShortWritable
}

object VOID extends ColumnType[Void, NullWritable](7, 0) {

  override def append(v: Void, buffer: ByteBuffer) = {}

  override def extract(currentPos: Int, buffer: ByteBuffer) = {
    throw new UnsupportedOperationException()
  }

  override def get(o: Object, oi: ObjectInspector) = null
  override def extractInto(currentPos: Int, buffer: ByteBuffer, writable: NullWritable) = {}
  override def newWritable() = NullWritable.get
}

object STRING extends ColumnType[Text, Text](8, 8) {

  private val _bytesFld = {
    val f = classOf[Text].getDeclaredField("bytes")
    f.setAccessible(true)
    f
  }

  private val _lengthFld = {
    val f = classOf[Text].getDeclaredField("length")
    f.setAccessible(true)
    f
  }

  override def append(v: Text, buffer: ByteBuffer) {
    val length = v.getLength()
    buffer.putInt(length)
    buffer.put(v.getBytes(), 0, length)
  }

  override def extract(currentPos: Int, buffer: ByteBuffer) = {
    val t = new Text()
    extractInto(currentPos, buffer, t)
    t
  }

  override def get(o: Object, oi: ObjectInspector): Text = {
    oi.asInstanceOf[StringObjectInspector].getPrimitiveWritableObject(o)
  }

  override def actualSize(v: Text) = v.getLength() + 4

  def extractInto(currentPos: Int, buffer: ByteBuffer, writable: Text) = {
    val length = buffer.getInt()
    var b = _bytesFld.get(writable).asInstanceOf[Array[Byte]]
    if (b == null || b.length < length) {
      b = new Array[Byte](length)
      _bytesFld.set(writable, b)
    }
    buffer.get(b, 0, length)
    _lengthFld.set(writable, length)
  }

  def newWritable() = new Text
  override def clone(v: Text) = {
    val t = new Text()
    t.set(v)
    t
  }
}

object TIMESTAMP extends ColumnType[Timestamp, TimestampWritable](9, 12) {

  override def append(v: Timestamp, buffer: ByteBuffer) = {
    buffer.putLong(v.getTime())
    buffer.putInt(v.getNanos())
  }

  override def extract(currentPos: Int, buffer: ByteBuffer) = {
    val ts = new Timestamp(0)
    ts.setTime(buffer.getLong())
    ts.setNanos(buffer.getInt())
    ts
  }

  override def get(o: Object, oi: ObjectInspector): Timestamp = {
    oi.asInstanceOf[TimestampObjectInspector].getPrimitiveJavaObject(o)
  }

  def extractInto(currentPos: Int, buffer: ByteBuffer, writable: TimestampWritable) = {
    writable.set(extract(currentPos, buffer))
  }

  def newWritable() = new TimestampWritable
}

object BINARY extends ColumnType[BytesWritable, BytesWritable](10, 16) {

  private val _bytesFld = {
    val f = classOf[BytesWritable].getDeclaredField("bytes")
    f.setAccessible(true)
    f
  }
  private val _lengthFld = {
    val f = classOf[BytesWritable].getDeclaredField("size")
    f.setAccessible(true)
    f
  }
  override def append(v: BytesWritable, buffer: ByteBuffer) = {
    val length = v.getLength()
    buffer.putInt(length)
    buffer.put(v.getBytes(), 0, length)
  }

  override def extract(currentPos: Int, buffer: ByteBuffer) = {
    throw new UnsupportedOperationException()
  }

  override def get(o: Object, oi: ObjectInspector): BytesWritable = {
    if (o.isInstanceOf[LazyBinary]) {
      o.asInstanceOf[LazyBinary].getWritableObject()
    } else if (o.isInstanceOf[BytesWritable]) {
      o.asInstanceOf[BytesWritable]
    } else {
      throw new UnsupportedOperationException("Unknown binary type " + oi)
    }
  }

  def extractInto(currentPos: Int, buffer: ByteBuffer, writable: BytesWritable) = {
    val length = buffer.getInt()
    var b = _bytesFld.get(writable).asInstanceOf[Array[Byte]]
    if (b == null || b.length < length) {
      b = new Array[Byte](length)
      _bytesFld.set(writable, b)
    }
    buffer.get(b, 0, length)
    _lengthFld.set(writable, length)
  }

  def newWritable() = new BytesWritable
}

object GENERIC extends ColumnType[ByteStream.Output, ByteArrayRef](11, 16) {

  override def append(v: ByteStream.Output, buffer: ByteBuffer) {
    val length = v.getCount()
    buffer.putInt(length)
    buffer.put(v.getData(), 0, length)
  }

  override def extract(currentPos: Int, buffer: ByteBuffer) = {
   throw new UnsupportedOperationException() 
  }

  override def get(o: Object, oi: ObjectInspector) = {
    o.asInstanceOf[ByteStream.Output]
  }

  def extractInto(currentPos: Int, buffer: ByteBuffer, writable: ByteArrayRef) = {
    val length = buffer.getInt()
    val a = new Array[Byte](length)
    buffer.get(a, 0, length)
    writable.setData(a)
  }

  def newWritable() = new ByteArrayRef
}

object ColumnBuilder {

  def create(columnOi: ObjectInspector): ColumnBuilder[_] = {
    val v = columnOi.getCategory match {
      case ObjectInspector.Category.PRIMITIVE => {
        columnOi.asInstanceOf[PrimitiveObjectInspector].getPrimitiveCategory match {
          case PrimitiveCategory.BOOLEAN   => new BooleanColumnBuilder
          case PrimitiveCategory.INT       => new IntColumnBuilder
          case PrimitiveCategory.LONG      => new LongColumnBuilder
          case PrimitiveCategory.FLOAT     => new FloatColumnBuilder
          case PrimitiveCategory.DOUBLE    => new DoubleColumnBuilder
          case PrimitiveCategory.STRING    => new StringColumnBuilder
          case PrimitiveCategory.SHORT     => new ShortColumnBuilder
          case PrimitiveCategory.BYTE      => new ByteColumnBuilder
          case PrimitiveCategory.TIMESTAMP => new TimestampColumnBuilder
          case PrimitiveCategory.BINARY    => new BinaryColumnBuilder
         
          // TODO: add decimal column.
          case _ => throw new Exception(
            "Invalid primitive object inspector category" + columnOi.getCategory)
        }
      }
      case _ => new GenericColumnBuilder(columnOi)
    }
    v
  }
}
