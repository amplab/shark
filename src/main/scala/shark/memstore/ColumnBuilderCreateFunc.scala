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

package shark.memstore

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory

import shark.memstore.Column._
import shark.memstore.ColumnStats._


object ColumnBuilderCreateFunc {

  type TYPE = (ObjectInspector, Int) => ColumnBuilder

  val uncompressedArrayFormat: TYPE = (oi: ObjectInspector, initialSize: Int) => {
    import UncompressedColumnFormat._
    oi.getCategory match {
      case ObjectInspector.Category.PRIMITIVE => {
        oi.asInstanceOf[PrimitiveObjectInspector].getPrimitiveCategory match {
          case PrimitiveCategory.BOOLEAN =>
            new BooleanColumnBuilder(new BooleanColumnFormat(initialSize), BooleanColumnNoStats)
          case PrimitiveCategory.BYTE =>
            new ByteColumnBuilder(new ByteColumnFormat(initialSize), ByteColumnNoStats)
          case PrimitiveCategory.SHORT =>
            new ShortColumnBuilder(new ShortColumnFormat(initialSize), ShortColumnNoStats)
          case PrimitiveCategory.INT =>
            new IntColumnBuilder(new IntColumnFormat(initialSize), IntColumnNoStats)
          case PrimitiveCategory.LONG =>
            new LongColumnBuilder(new LongColumnFormat(initialSize), LongColumnNoStats)
          case PrimitiveCategory.FLOAT =>
            new FloatColumnBuilder(new FloatColumnFormat(initialSize), FloatColumnNoStats)
          case PrimitiveCategory.DOUBLE =>
            new DoubleColumnBuilder(new DoubleColumnFormat(initialSize), DoubleColumnNoStats)
          case PrimitiveCategory.STRING =>
            new TextColumnBuilder(new TextColumnFormat(initialSize), TextColumnNoStats)
          case PrimitiveCategory.VOID => new VoidColumnBuilder()
          case _ => throw new Exception("Invalid primitive object inspector category")
        }
      }
      case _ => new LazyColumnBuilder(oi, initialSize)
    }
  }

  val uncompressedArrayFormatWithStats: TYPE = (oi: ObjectInspector, initialSize: Int) => {
    import UncompressedColumnFormat._
    oi.getCategory match {
      case ObjectInspector.Category.PRIMITIVE => {
        oi.asInstanceOf[PrimitiveObjectInspector].getPrimitiveCategory match {
          case PrimitiveCategory.BOOLEAN =>
            new BooleanColumnBuilder(new BooleanColumnFormat(initialSize), new BooleanColumnStats)
          case PrimitiveCategory.BYTE =>
            new ByteColumnBuilder(new ByteColumnFormat(initialSize), new ByteColumnStats)
          case PrimitiveCategory.SHORT =>
            new ShortColumnBuilder(new ShortColumnFormat(initialSize), new ShortColumnStats)
          case PrimitiveCategory.INT =>
            new IntColumnBuilder(new IntColumnFormat(initialSize), new IntColumnStats)
          case PrimitiveCategory.LONG =>
            new LongColumnBuilder(new LongColumnFormat(initialSize), new LongColumnStats)
          case PrimitiveCategory.FLOAT =>
            new FloatColumnBuilder(new FloatColumnFormat(initialSize), new FloatColumnStats)
          case PrimitiveCategory.DOUBLE =>
            new DoubleColumnBuilder(new DoubleColumnFormat(initialSize), new DoubleColumnStats)
          case PrimitiveCategory.STRING =>
            new TextColumnBuilder(new TextColumnFormat(initialSize), new TextColumnStats)
          case PrimitiveCategory.VOID => new VoidColumnBuilder()
          case _ => throw new Exception("Invalid primitive object inspector category")
        }
      }
      case _ => new LazyColumnBuilder(oi, initialSize)
    }
  }

  val compressedFormatWithStats: TYPE = (oi: ObjectInspector, initialSize: Int) => {
    import UncompressedColumnFormat._
    oi.getCategory match {
      case ObjectInspector.Category.PRIMITIVE => {
        oi.asInstanceOf[PrimitiveObjectInspector].getPrimitiveCategory match {
          case PrimitiveCategory.INT =>
            val cf = new CompressedIntColumnFormat(initialSize)
            new IntColumnBuilder(cf, cf.stats)
          case PrimitiveCategory.STRING =>
            val cf = new CompressedTextColumnFormat(initialSize, 128)
            new TextColumnBuilder(cf, new TextColumnStats)
          case _ => uncompressedArrayFormatWithStats(oi, initialSize)
        }
      }
      case _ => uncompressedArrayFormatWithStats(oi, initialSize)
    }
  }
}
