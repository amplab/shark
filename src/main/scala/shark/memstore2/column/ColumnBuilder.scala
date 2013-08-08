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



trait ColumnBuilder[T] {

  def t: ColumnType[T]
  private var _buffer: ByteBuffer = _
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
  def initialize(initialSize: Int):ByteBuffer = {
    val size = if(initialSize == 0) 1024 else initialSize
    _buffer = ByteBuffer.allocate(size*t.size + 4)
    _buffer.order(ByteOrder.nativeOrder())
    _buffer.putInt(t.index)
  }
  
  protected def growIfNeeded(orig: ByteBuffer, loadFactor: Double = 0.75): ByteBuffer = {
    val capacity = orig.capacity()
    if (orig.remaining() < (1 - loadFactor) * capacity) {
      //grow in steps of 1KB
      val s = 2*orig.capacity()
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

class DefaultColumnBuilder[T](val stats: ColumnStats[T], val t: ColumnType[T]) 
  extends NullableColumnBuilder[T]{}


trait CompressedColumnBuilder[T] extends ColumnBuilder[T] {
  var compressionSchemes:Seq[CompressionAlgorithm] = Seq(new RLE())
  def shouldApply(scheme: CompressionAlgorithm): Boolean = {
    scheme.compressibilityScore < 0.2
  }
  override protected def gatherStats(v: T) = {
    compressionSchemes.foreach { scheme =>
      if (scheme.supportsType(t)) {
        scheme.gatherStatsForCompressability(v, t)
      }
    }
  }
  
  override def build = {
    val b = super.build
    val schemes = compressionSchemes.sortBy(s => s.compressibilityScore)
    if (schemes.isEmpty) {
      b
    } else {
      val candidateScheme = schemes.head
      if (shouldApply(candidateScheme)) {
        candidateScheme.compress(b, t)
      } else {
        b
      }
    }
  }
}

abstract class ColumnType[T](val index: Int, val size: Int) {
  def extract(currentPos: Int, buffer: ByteBuffer): T
  def append(v: T, buffer: ByteBuffer)
  def get(o: Object, oi: ObjectInspector): T
  def actualSize(v: T) = size
}

object INT extends ColumnType[Int](0, 4) {
  override def append(v: Int, buffer: ByteBuffer) = {
    buffer.putInt(v)
  }

  override def extract(currentPos: Int, buffer: ByteBuffer) = {
    buffer.getInt()
  }
  override def get(o: Object, oi: ObjectInspector): Int = {
    oi.asInstanceOf[IntObjectInspector].get(o)
  }
}

object LONG extends ColumnType[Long](1, 8) {
  override def append(v: Long, buffer: ByteBuffer) = {
    buffer.putLong(v)
  }

  override def extract(currentPos: Int, buffer: ByteBuffer) = {
    buffer.getLong()
  }
  override def get(o: Object, oi: ObjectInspector): Long = {
    oi.asInstanceOf[LongObjectInspector].get(o)
  }
}

object FLOAT extends ColumnType[Float](2, 4) {
  override def append(v: Float, buffer: ByteBuffer) = {
    buffer.putFloat(v)
  }

  override def extract(currentPos: Int, buffer: ByteBuffer) = {
    buffer.getFloat()
  }
   override def get(o: Object, oi: ObjectInspector): Float = {
    oi.asInstanceOf[FloatObjectInspector].get(o)
  }
}

object DOUBLE extends ColumnType[Double](3, 8) {
  override def append(v: Double, buffer: ByteBuffer) = {
    buffer.putDouble(v)
  }

  override def extract(currentPos: Int, buffer: ByteBuffer) = {
    buffer.getDouble()
  }
  override def get(o: Object, oi: ObjectInspector): Double = {
    oi.asInstanceOf[DoubleObjectInspector].get(o)
  }
}

object BOOLEAN extends ColumnType[Boolean](4, 1) {
  override def append(v: Boolean, buffer: ByteBuffer) = {
    buffer.put(if (v) 1.toByte else 0.toByte)
  }

  override def extract(currentPos: Int, buffer: ByteBuffer) = {
    if (buffer.get() == 1) true else false
  }
  override def get(o: Object, oi: ObjectInspector): Boolean = {
    oi.asInstanceOf[BooleanObjectInspector].get(o)
  }
}

object BYTE extends ColumnType[Byte](5, 1) {
  override def append(v: Byte, buffer: ByteBuffer) = {
    buffer.put(v)
  }
  
  override def extract(currentPos: Int, buffer: ByteBuffer) = {
    buffer.get()
  }
  override def get(o: Object, oi: ObjectInspector): Byte = {
    oi.asInstanceOf[ByteObjectInspector].get(o)
  }
}

object SHORT extends ColumnType[Short](6, 2) {
  override def append(v: Short, buffer: ByteBuffer) = {
    buffer.putShort(v)
  }
  
  override def extract(currentPos: Int, buffer: ByteBuffer) = {
    buffer.getShort()
  }
  override def get(o: Object, oi: ObjectInspector): Short = {
    oi.asInstanceOf[ShortObjectInspector].get(o)
  }
}

object VOID extends ColumnType[Void](7, 0) {
  override def append(v: Void, buffer: ByteBuffer) = {}

  override def extract(currentPos: Int, buffer: ByteBuffer) = {
    throw new UnsupportedOperationException()
  }
  override def get(o: Object, oi: ObjectInspector) = null
}

object STRING extends ColumnType[Text](8, 8) {
  override def append(v: Text, buffer: ByteBuffer) {
    val length = v.getLength()
    buffer.putInt(length)
    buffer.put(v.getBytes(), 0, length)
  }

  override def extract(currentPos: Int, buffer: ByteBuffer) = {
    val length = buffer.getInt()
    val t = new Text()
    val a = new Array[Byte](length)
    buffer.get(a, 0, length)
    t.set(a)
    t
  }
  override def get(o: Object, oi: ObjectInspector): Text = {
    oi.asInstanceOf[StringObjectInspector].getPrimitiveWritableObject(o)
  }
  override def actualSize(v: Text) = v.getLength() + 4
}

object TIMESTAMP extends ColumnType[Timestamp](9, 12) {
  override def append(v: Timestamp, buffer: ByteBuffer) = {
    buffer.putLong(v.getTime())
    buffer.putInt(v.getNanos())
  }

  override def extract(currentPos: Int, buffer: ByteBuffer) = {
    throw new UnsupportedOperationException()
  }
  override def get(o: Object, oi: ObjectInspector): Timestamp = {
    oi.asInstanceOf[TimestampObjectInspector].getPrimitiveJavaObject(o)
  }
}

object BINARY extends ColumnType[BytesWritable](10, 16) {
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
}

object GENERIC extends ColumnType[ByteStream.Output](11, 16) {
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
