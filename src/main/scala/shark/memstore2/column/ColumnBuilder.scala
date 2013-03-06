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

import javaewah.EWAHCompressedBitmap

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory


trait ColumnBuilder[T] {
  protected var _nulls: EWAHCompressedBitmap = null

  def initialize(initialSize: Int) {
    _nulls = new EWAHCompressedBitmap
  }

  def append(o: Object, oi: ObjectInspector)

  def append(v: T)

  def appendNull()

  def build: ByteBuffer
}


object ColumnBuilder {

  def create(columnOi: ObjectInspector): ColumnBuilder[_] = {
    columnOi.getCategory match {
      case ObjectInspector.Category.PRIMITIVE => {
        columnOi.asInstanceOf[PrimitiveObjectInspector].getPrimitiveCategory match {
          case PrimitiveCategory.BOOLEAN   => new BooleanColumnBuilder
          case PrimitiveCategory.BYTE      => new ByteColumnBuilder
          case PrimitiveCategory.SHORT     => new ShortColumnBuilder
          case PrimitiveCategory.INT       => new IntColumnBuilder
          case PrimitiveCategory.LONG      => new LongColumnBuilder
          case PrimitiveCategory.FLOAT     => new FloatColumnBuilder
          case PrimitiveCategory.DOUBLE    => new DoubleColumnBuilder
          case PrimitiveCategory.STRING    => new StringColumnBuilder
          case PrimitiveCategory.VOID      => new VoidColumnBuilder
          case PrimitiveCategory.TIMESTAMP => new TimestampColumnBuilder
          // TODO: add binary column.
          // TODO: add decimal column.
          case _ => throw new Exception("Invalid primitive object inspector category")
        }
      }
      case _ => new ComplexColumnBuilder(columnOi)
    }
  }
}
