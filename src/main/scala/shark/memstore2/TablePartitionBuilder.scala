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

package shark.memstore2

import java.io.{DataInput, DataOutput}
import java.nio.ByteBuffer
import java.util.{List => JList}

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory
import org.apache.hadoop.hive.serde2.objectinspector.StructField
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector
import org.apache.hadoop.io.Writable


/**
 * Used to build a TablePartition. This is used in the serializer to convert a
 * partition of data into columnar format and to generate a TablePartition.
 */
class TablePartitionBuilder(oi: StructObjectInspector, initialColumnSize: Int)
  extends Writable {

  var numRows: Long = 0
  val fields: JList[_ <: StructField] = oi.getAllStructFieldRefs

  val columnBuilders = Array.tabulate[ColumnBuilder[_]](fields.size) { i =>
    val columnBuilder = TablePartitionBuilder.createColumnBuilder(
      fields.get(i).getFieldObjectInspector)
    columnBuilder.initialize(initialColumnSize)
    columnBuilder
  }

  def incrementRowCount() {
    numRows += 1
  }

  def append(id: Int, o: Object, oi: ObjectInspector) {
    columnBuilders(id).append(o, oi)
  }

  def build: TablePartition = {
    new TablePartition(numRows, columnBuilders.map(_.build))
  }

  // We don't use these, but want to maintain Writable interface for SerDe
  override def write(out: DataOutput) {}
  override def readFields(in: DataInput) {}
}


object TablePartitionBuilder {

  def createColumnBuilder(columnOi: ObjectInspector): ColumnBuilder[_] = {
    columnOi.getCategory match {
      case ObjectInspector.Category.PRIMITIVE => {
        columnOi.asInstanceOf[PrimitiveObjectInspector].getPrimitiveCategory match {
          case PrimitiveCategory.BOOLEAN => new BooleanColumnBuilder
          case PrimitiveCategory.BYTE    => new ByteColumnBuilder
          case PrimitiveCategory.SHORT   => new ShortColumnBuilder
          case PrimitiveCategory.INT     => new IntColumnBuilder
          case PrimitiveCategory.LONG    => new LongColumnBuilder
          case PrimitiveCategory.FLOAT   => new FloatColumnBuilder
          case PrimitiveCategory.DOUBLE  => new DoubleColumnBuilder
          case PrimitiveCategory.STRING  => new StringColumnBuilder
          case PrimitiveCategory.VOID    => null
          // TODO: add timestamp and binary column.
          case _ => throw new Exception("Invalid primitive object inspector category")
        }
      }
      case _ => null // new LazyColumnBuilder(oi, initialSize)
    }
  }
}
