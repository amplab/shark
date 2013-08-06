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
import shark.memstore2.column._
import org.apache.hadoop.io.BytesWritable
import java.sql.Timestamp
import org.apache.hadoop.io.Text
import org.apache.hadoop.hive.serde2.ByteStream


trait ColumnBuilder[T] {

  def t: ColumnType[T]
  protected var _buffer: ByteBuffer = _
  protected var _stats: ColumnStats[T] = _

  def append(o: Object, oi: ObjectInspector): Unit = {
    _buffer = growIfNeeded(_buffer)
    val v = get(o, oi)
    append(v)
    _stats.append(v)
  }

  def get(o: Object, oi: ObjectInspector): T
  
  def append(v: T)

  def stats = _stats

  def build: ByteBuffer = {
    _buffer.limit(_buffer.position())
    _buffer.rewind()
    _buffer
  }

  /**
   * Initialize with an approximate lower bound on the expected number
   * of elements in this column.
   */
  def initialize(initialSize: Int) {
    val size = if(initialSize == 0) 1024 else initialSize
    this._buffer = ByteBuffer.allocate(size*t.size + 4)
    _buffer.order(ByteOrder.nativeOrder())
    _buffer.putInt(t.index)
  }
  
  def growIfNeeded(orig: ByteBuffer, loadFactor: Double = 0.75): ByteBuffer = {
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

case class ColumnType[T](index: Int, size: Int)
object INT extends ColumnType[Int](0, 4)
object LONG extends ColumnType[Long](1, 8)
object FLOAT extends ColumnType[Float](2, 4)
object DOUBLE extends ColumnType[Double](3, 8)
object BOOLEAN extends ColumnType[Boolean](4, 1)
object BYTE extends ColumnType[Byte](5, 1)
object SHORT extends ColumnType[Short](6, 4)
object VOID extends ColumnType[Void](7, 0)
object STRING extends ColumnType[Text](8, 8)
object TIMESTAMP extends ColumnType[Timestamp](9, 12)
object BINARY extends ColumnType[BytesWritable](10, 16)
object GENERIC extends ColumnType[ByteStream.Output](11, 16)


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
