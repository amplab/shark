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

import java.util.{List => JList, Properties}
import scala.collection.JavaConversions._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hdfs.DFSConfigKeys
import org.apache.hadoop.hive.serde2.{ByteStream, SerDe, SerDeStats}
import org.apache.hadoop.hive.serde2.`lazy`.{LazyFactory, LazySimpleSerDe}
import org.apache.hadoop.hive.serde2.`lazy`.LazySimpleSerDe.SerDeParameters
import org.apache.hadoop.hive.serde2.objectinspector.{ListObjectInspector, MapObjectInspector,
  ObjectInspector, PrimitiveObjectInspector, StructField, StructObjectInspector,
  UnionObjectInspector}
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory
import org.apache.hadoop.io.Writable
import shark.LogHelper
import shark.SharkConfVars
import shark.memstore2.column.ColumnBuilder._
import shark.memstore2.column._


class ColumnarSerDe extends SerDe with LogHelper {

  var objectInspector: StructObjectInspector = _
  var tablePartitionBuilder: TablePartitionBuilder = _
  var serDeParams: SerDeParameters = _
  var numRows: Int = _
  var shouldCompress: Boolean = _
  val serializeStream = new ByteStream.Output

  override def initialize(conf: Configuration, tbl: Properties) {
    serDeParams = LazySimpleSerDe.initSerdeParams(conf, tbl, this.getClass.getName)
    // Create oi & writable.
    objectInspector = ColumnarStructObjectInspector(serDeParams)

    // This null check is needed because Hive's SemanticAnalyzer.genFileSinkPlan() creates
    // an instance of the table's StructObjectInspector by creating an instance SerDe, which
    // it initializes by passing a 'null' argument for 'conf'.
    if (conf != null) {
      numRows = SharkConfVars.getIntVar(conf, SharkConfVars.COLUMN_INITIALSIZE)
      shouldCompress = SharkConfVars.getBoolVar(conf, SharkConfVars.COLUMNAR_COMPRESSION)
      logInfo("should compress " + shouldCompress)
      logInfo("num rows " + numRows)
    }
  }

  override def deserialize(blob: Writable): Object =
    throw new UnsupportedOperationException("ColumnarSerDe.deserialize()")

  override def getSerDeStats: SerDeStats = {
    // TODO: Stats are not collected yet.
    new SerDeStats
  }

  override def getObjectInspector: ObjectInspector = objectInspector

  override def getSerializedClass: Class[_ <: Writable] = classOf[TablePartitionBuilder]

  override def serialize(obj: Object, objInspector: ObjectInspector): Writable = {
    if (tablePartitionBuilder == null) {
      logInfo("should compress " + shouldCompress)
      logInfo("num rows " + numRows)
      tablePartitionBuilder = new TablePartitionBuilder(objectInspector, numRows, shouldCompress)
    }

    tablePartitionBuilder.incrementRowCount()
    val soi = objInspector.asInstanceOf[StructObjectInspector]
    val fields: JList[_ <: StructField] = soi.getAllStructFieldRefs

    var i = 0
    while (i < fields.size) {
      val field = fields.get(i)
      val fieldOI: ObjectInspector = field.getFieldObjectInspector
      fieldOI.getCategory match {
        case ObjectInspector.Category.PRIMITIVE =>
          tablePartitionBuilder.append(i, soi.getStructFieldData(obj, field), fieldOI)
        case other => {
          // We use LazySimpleSerDe to serialize nested data
          LazySimpleSerDe.serialize(
            serializeStream, soi.getStructFieldData(obj, field),
            fieldOI,
            serDeParams.getSeparators(),
            1,
            serDeParams.getNullSequence(),
            serDeParams.isEscaped(),
            serDeParams.getEscapeChar(),
            serDeParams.getNeedsEscape())
          tablePartitionBuilder.append(i, serializeStream, fieldOI)
          serializeStream.reset()
        }
      }
      i += 1
    }
    tablePartitionBuilder
  }
}


object ColumnarSerDe {

  // Determine the size of the field.
  def getFieldSize(oi: ObjectInspector): Int = {
    val size = oi.getCategory match {
      case ObjectInspector.Category.PRIMITIVE => {
        oi.asInstanceOf[PrimitiveObjectInspector].getPrimitiveCategory match {
          case PrimitiveCategory.VOID      => VOID.size
          case PrimitiveCategory.BOOLEAN   => BOOLEAN.size
          case PrimitiveCategory.BYTE      => BYTE.size
          case PrimitiveCategory.SHORT     => SHORT.size
          case PrimitiveCategory.INT       => INT.size
          case PrimitiveCategory.LONG      => LONG.size
          case PrimitiveCategory.FLOAT     => FLOAT.size
          case PrimitiveCategory.DOUBLE    => DOUBLE.size
          case PrimitiveCategory.TIMESTAMP => TIMESTAMP.size
          case PrimitiveCategory.STRING    => STRING.size
          case PrimitiveCategory.BINARY    => BINARY.size
          // TODO: add decimal type.
          case _ => throw new Exception(
            "Invalid primitive object inspector category" + oi + " " +
            oi.asInstanceOf[PrimitiveObjectInspector].getPrimitiveCategory)
        }
      }
      case ObjectInspector.Category.LIST   => 16
      case ObjectInspector.Category.UNION  => 16
      case ObjectInspector.Category.MAP    => 16
      case ObjectInspector.Category.STRUCT => {
        val fieldRefs: JList[_ <: StructField] =
          oi.asInstanceOf[StructObjectInspector].getAllStructFieldRefs
        fieldRefs.foldLeft(0)((sum, structField) =>
          sum + getFieldSize(structField.getFieldObjectInspector))
      }
      case _ => throw new Exception(
        "Invalid object inspector category " + oi + " " + oi.getCategory)
    }
    size
  }
}
