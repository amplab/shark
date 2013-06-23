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
import shark.memstore2.column.ColumnIterator


class ColumnarSerDe extends SerDe with LogHelper {

  var objectInspector: StructObjectInspector = _
  var tablePartitionBuilder: TablePartitionBuilder = _
  var serDeParams: SerDeParameters = _
  var initialColumnSize: Int = _
  var columnarComprString: String = _
  var columnarComprInt: String = _
  val serializeStream = new ByteStream.Output

  override def initialize(conf: Configuration, tbl: Properties) {
    serDeParams = LazySimpleSerDe.initSerdeParams(conf, tbl, this.getClass.getName)
    // Create oi & writable.
    objectInspector = ColumnarStructObjectInspector(serDeParams)

    // This null check is needed because Hive's SemanticAnalyzer.genFileSinkPlan() creates
    // an instance of the table's StructObjectInspector by creating an instance SerDe, which
    // it initializes by passing a 'null' argument for 'conf'.
    if (conf != null) {
      initialColumnSize = SharkConfVars.getIntVar(conf, SharkConfVars.COLUMN_INITIALSIZE)
      columnarComprString = SharkConfVars.getVar(conf, SharkConfVars.COLUMNAR_COMPR_STRING)
      // println("From ColumnarSerde " + columnarComprString)
      // val columnarComprString2 = conf.get(SharkConfVars.COLUMNAR_COMPR_STRING.varname,
      //   conf.get("shark.columnar.compr.string", "none"))
      // println("From ColumnarSerde2 " + columnarComprString2)
      // val columnarComprString3 = SharkConfVars.COLUMNAR_COMPR_STRING
      // println("From ColumnarSerde3 " + columnarComprString3)
      columnarComprInt = SharkConfVars.getVar(conf, SharkConfVars.COLUMNAR_COMPR_INT)

      if (initialColumnSize == - 1) {
        // Approximate the size of a partition by using the HDFS "dfs.block.size" config.
        // Try both "dfs.block.size" and "dfs.blocksize" (from DFS_BLOCK_SIZE_KEY), which
        // is the new name since Hadoop 0.21.0
        val partitionSize = conf.getLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY,
          conf.getLong("dfs.block.size", DFSConfigKeys.DFS_BLOCK_SIZE_DEFAULT))

        logInfo("Estimated size of partition to cache is " + partitionSize)

        // Estimate the initial capacity for ArrayList columns using:
        // partition_size / (num_columns * avg_field_size).
        val rowSize = ColumnarSerDe.getFieldSize(objectInspector).toLong
        initialColumnSize = (partitionSize / rowSize).toInt


        logInfo("Estimated size of each row is: " + rowSize)
      }
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
      tablePartitionBuilder = new TablePartitionBuilder(objectInspector,
        initialColumnSize, columnarComprString, columnarComprInt)
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
          case PrimitiveCategory.VOID      => ColumnIterator.NULL_SIZE
          case PrimitiveCategory.BOOLEAN   => ColumnIterator.BOOLEAN_SIZE
          case PrimitiveCategory.BYTE      => ColumnIterator.BYTE_SIZE
          case PrimitiveCategory.SHORT     => ColumnIterator.SHORT_SIZE
          case PrimitiveCategory.INT       => ColumnIterator.INT_SIZE
          case PrimitiveCategory.LONG      => ColumnIterator.LONG_SIZE
          case PrimitiveCategory.FLOAT     => ColumnIterator.FLOAT_SIZE
          case PrimitiveCategory.DOUBLE    => ColumnIterator.DOUBLE_SIZE
          case PrimitiveCategory.TIMESTAMP => ColumnIterator.TIMESTAMP_SIZE
          case PrimitiveCategory.STRING    => ColumnIterator.STRING_SIZE
          case PrimitiveCategory.BINARY    => ColumnIterator.BINARY_SIZE
          // TODO: add decimal type.
          case _ => throw new Exception(
            "Invalid primitive object inspector category" + oi + " " +
            oi.asInstanceOf[PrimitiveObjectInspector].getPrimitiveCategory)
        }
      }
      case ObjectInspector.Category.LIST   => ColumnIterator.COMPLEX_TYPE_SIZE
      case ObjectInspector.Category.UNION  => ColumnIterator.COMPLEX_TYPE_SIZE
      case ObjectInspector.Category.MAP    => ColumnIterator.COMPLEX_TYPE_SIZE
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
