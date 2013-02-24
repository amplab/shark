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
import shark.{LogHelper, SharkConfVars}
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableBinaryObjectInspector


class ColumnarSerDe(builderFunc: ColumnBuilderCreateFunc.TYPE) extends SerDe with LogHelper {

  def this() = this(null)

  var objectInspector: StructObjectInspector = _
  var tableStorageBuilder: TableStorageBuilder = _
  var serDeParams: SerDeParameters = _
  var initialColumnSize: Int = _
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

  override def getSerializedClass: Class[_ <: Writable] = classOf[TableStorageBuilder]

  override def serialize(obj: Object, objInspector: ObjectInspector): Writable = {
    if (tableStorageBuilder == null) {
      tableStorageBuilder = new TableStorageBuilder(
        objectInspector, initialColumnSize, builderFunc)
    }

    tableStorageBuilder.incrementRowCount()
    val soi = objInspector.asInstanceOf[StructObjectInspector]
    val fields: JList[_ <: StructField] = soi.getAllStructFieldRefs

    var i = 0
    while (i < fields.size) {
      val field = fields.get(i)
      val fieldOI: ObjectInspector = field.getFieldObjectInspector
      fieldOI.getCategory match {
        case ObjectInspector.Category.PRIMITIVE =>
          tableStorageBuilder.append(i, soi.getStructFieldData(obj, field), fieldOI)
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
          tableStorageBuilder.append(i, serializeStream, fieldOI)
          serializeStream.reset()
        }
      }
      i += 1
    }
    tableStorageBuilder
  }

  def stats: TableStats = {
    if (tableStorageBuilder == null) new TableStats(null, 0)
    else tableStorageBuilder.stats
  }
}


object ColumnarSerDe {

  class Basic extends ColumnarSerDe(ColumnBuilderCreateFunc.uncompressedArrayFormat)
  class WithStats extends ColumnarSerDe(ColumnBuilderCreateFunc.uncompressedArrayFormatWithStats)
  class Compressed extends ColumnarSerDe(ColumnBuilderCreateFunc.compressedFormatWithStats)

  // Sizes of primitive types
  val BOOLEAN_SIZE = 1
  val BYTE_SIZE = 1
  val SHORT_SIZE = 2
  val INT_SIZE = 4
  val LONG_SIZE = 8
  val FLOAT_SIZE = 4
  val DOUBLE_SIZE = 8

  // Strings are assumed to be 16 bytes on average.
  val STRING_SIZE = 16

  // Void columns are represented by NullWritable, which is a singleton, so it's not
  // large enough to matter.
  val NULL_SIZE = 0

  // Estimates for average sizes of Lists and Maps.
  val MAP_SIZE = 5
  val LIST_SIZE = 5

  // Determine average field size from the ObjectInspector passed. Should remove parts for
  // nested data once those are supported.
  def getFieldSize(oi: ObjectInspector): Int = {
    val size = oi.getCategory match {
      case ObjectInspector.Category.PRIMITIVE => {
        oi.asInstanceOf[PrimitiveObjectInspector].getPrimitiveCategory match {
          case PrimitiveCategory.BOOLEAN => BOOLEAN_SIZE
          case PrimitiveCategory.BYTE => BYTE_SIZE
          case PrimitiveCategory.SHORT => SHORT_SIZE
          case PrimitiveCategory.INT => INT_SIZE
          case PrimitiveCategory.LONG => LONG_SIZE
          case PrimitiveCategory.FLOAT => FLOAT_SIZE
          case PrimitiveCategory.DOUBLE => DOUBLE_SIZE
          case PrimitiveCategory.STRING => STRING_SIZE
          case PrimitiveCategory.VOID => NULL_SIZE
          case PrimitiveCategory.BINARY => 16
          case _ => throw new Exception("Invalid primitive object inspector category" + oi + " " + oi.asInstanceOf[PrimitiveObjectInspector].getPrimitiveCategory)
        }
      }
      case ObjectInspector.Category.LIST => {
        val listOISize = getFieldSize(
          oi.asInstanceOf[ListObjectInspector].getListElementObjectInspector)
        LIST_SIZE * listOISize
      }
      case ObjectInspector.Category.STRUCT => {
        val fieldRefs: JList[_ <: StructField] =
          oi.asInstanceOf[StructObjectInspector].getAllStructFieldRefs
        fieldRefs.foldLeft(0)((sum, structField) =>
          sum + getFieldSize(structField.getFieldObjectInspector))
      }
      case ObjectInspector.Category.UNION => {
        val unionOIs: JList[ObjectInspector] =
          oi.asInstanceOf[UnionObjectInspector].getObjectInspectors
        val unionOISizes = unionOIs.foldLeft(0)((sum, unionOI) =>
          sum + getFieldSize(unionOI))
        unionOISizes / unionOIs.size
      }
      case ObjectInspector.Category.MAP => {
        val mapOI = oi.asInstanceOf[MapObjectInspector]
        val keySize = getFieldSize(mapOI.getMapKeyObjectInspector())
        val valueSize = getFieldSize(mapOI.getMapValueObjectInspector())
        MAP_SIZE * (keySize + valueSize)
      }
      case _ => throw new Exception("Invalid primitive object inspector category" + oi.getCategory)
    }
    size
  }
}
