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
import org.apache.hadoop.hive.serde2.`lazy`.LazySimpleSerDe
import org.apache.hadoop.hive.serde2.`lazy`.LazySimpleSerDe.SerDeParameters
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector, PrimitiveObjectInspector,
  StructField, StructObjectInspector}
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory
import org.apache.hadoop.io.Writable

import shark.LogHelper
import shark.SharkConfVars
import shark.memstore2.column._


class ColumnarSerDe extends SerDe with LogHelper {

  var objectInspector: StructObjectInspector = _
  var tablePartitionBuilder: TablePartitionBuilder = _
  var serDeParams: SerDeParameters = _
  var estimatedNumRows: Int = _
  var shouldCompress: Boolean = _
  val serializeStream = new ByteStream.Output

  override def initialize(conf: Configuration, tbl: Properties) {
    serDeParams = LazySimpleSerDe.initSerdeParams(conf, tbl, this.getClass.getName)
    // Create oi & writable.
    objectInspector = ColumnarStructObjectInspector(serDeParams)

    // This null check is needed because Hive's SemanticAnalyzer.genFileSinkPlan() creates
    // an instance of the table's StructObjectInspector by creating an instance of SerDe, which
    // it initializes by passing a 'null' argument for 'conf'.
    if (conf != null) {
      var partitionSize = {
        SharkConfVars.getIntVar(conf, SharkConfVars.COLUMN_BUILDER_PARTITION_SIZE) * 1024 * 1024
      }.toLong
      if (partitionSize < 0) {
        partitionSize = conf.getLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY,
          conf.getLong("dfs.block.size", DFSConfigKeys.DFS_BLOCK_SIZE_DEFAULT))
      }
      val rowSize = ColumnarSerDe.getFieldSize(objectInspector).toLong
      estimatedNumRows = (partitionSize / rowSize).toInt
      shouldCompress = SharkConfVars.getBoolVar(conf, SharkConfVars.COLUMNAR_COMPRESSION)
      logInfo("Initializing column serde " +
        "with compression %s. Estimated partition size: %d; number of rows: %d"
        .format(if (shouldCompress) "on" else "off", partitionSize, estimatedNumRows))
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
      tablePartitionBuilder = new TablePartitionBuilder(objectInspector, estimatedNumRows,
        shouldCompress)
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
          case PrimitiveCategory.VOID      => VOID.defaultSize
          case PrimitiveCategory.BOOLEAN   => BOOLEAN.defaultSize
          case PrimitiveCategory.BYTE      => BYTE.defaultSize
          case PrimitiveCategory.SHORT     => SHORT.defaultSize
          case PrimitiveCategory.INT       => INT.defaultSize
          case PrimitiveCategory.LONG      => LONG.defaultSize
          case PrimitiveCategory.FLOAT     => FLOAT.defaultSize
          case PrimitiveCategory.DOUBLE    => DOUBLE.defaultSize
          case PrimitiveCategory.TIMESTAMP => TIMESTAMP.defaultSize
          case PrimitiveCategory.STRING    => STRING.defaultSize
          case PrimitiveCategory.BINARY    => BINARY.defaultSize
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
