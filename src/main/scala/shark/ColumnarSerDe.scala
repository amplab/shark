package shark

import java.io.{DataInput, DataOutput}
import java.util.Properties

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hdfs.DFSConfigKeys
import org.apache.hadoop.hive.serde2.{ByteStream, SerDe, SerDeStats, SerDeException}
import org.apache.hadoop.hive.serde2.`lazy`.{LazyFactory, LazySimpleSerDe}
import org.apache.hadoop.hive.serde2.`lazy`.LazySimpleSerDe.SerDeParameters
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory
import org.apache.hadoop.hive.serde2.objectinspector.StructField
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.UnionObjectInspector
import org.apache.hadoop.hive.serde2.typeinfo.{PrimitiveTypeInfo, TypeInfo}
import org.apache.hadoop.io.Writable

import scala.collection.JavaConversions._


class ColumnarSerDe extends SerDe with LogHelper {

  var cachedObjectInspector: StructObjectInspector = _
  var cachedWritable: ColumnarWritable = _
  var cachedStruct: ColumnarStruct = _
  var serDeParams: SerDeParameters = _
  var initialColumnSize: Int = _
  var stats : SerDeStats = _
  val serializeStream = new ByteStream.Output()

  def initialize(conf: Configuration, tbl: Properties) {
    stats = new SerDeStats
    serDeParams = LazySimpleSerDe.initSerdeParams(conf, tbl, getClass().getName())
    // Create oi & writable.
    cachedObjectInspector = ColumnarStructObjectInspector(serDeParams)

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

        logDebug("Estimated size of partition to cache is " + partitionSize)

        // Estimate the initial capacity for ArrayList columns using:
        // partition_size / (num_columns * avg_field_size).
        val rowSize = ColumnarSerDe.getFieldSize(cachedObjectInspector).toLong
        initialColumnSize = (partitionSize / rowSize).toInt

        logDebug("Estimated size of each row is: " + rowSize)
      }
    }
  }

  def deserialize(blob: Writable): Object = {
    if (cachedStruct == null)
      cachedStruct = new ColumnarStruct(blob.asInstanceOf[ColumnarWritable])
    cachedStruct.initializeNextRow()
    cachedStruct
  }

  def getObjectInspector(): ObjectInspector = {
    return cachedObjectInspector
  }

  def getSerializedClass(): Class[_ <: Writable] = {
    return classOf[ColumnarWritable]
  }

  def getSerDeStats(): SerDeStats = {
    // TODO: Stats are not collected yet.
    return new SerDeStats
  }

  def serialize(obj: Object, objInspector: ObjectInspector): Writable = {

    if (cachedWritable == null) {
      cachedWritable = new ColumnarWritable(cachedObjectInspector, initialColumnSize)
    }

    val soi = objInspector.asInstanceOf[StructObjectInspector]
    val fields = soi.getAllStructFieldRefs
    //Doesn't handle complex things yet
    var i = 0
    while (i < fields.size) {
      val field = fields.get(i)
      val fieldOI = field.getFieldObjectInspector
      fieldOI.getCategory match {
        case Category.PRIMITIVE =>
          cachedWritable.add(i, soi.getStructFieldData(obj, field), fieldOI)
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
          cachedWritable.add(i, serializeStream, fieldOI)
          serializeStream.reset()
        }
      }
      i += 1
    }
    cachedWritable
  }
}

class ColumnarWritable(oi: StructObjectInspector, initialColumnSize: Int)

  extends Writable {

  val fields = oi.getAllStructFieldRefs
  val columns = new Array[Column](fields.size)

  (0 until columns.length).foreach { i =>
    columns(i) = Column(fields.get(i).getFieldObjectInspector, initialColumnSize)
  }

  def getField(id: Int, rowId: Int): Object = {
    columns(id)(rowId)
  }

  def add(id: Int, o: Object, oi: ObjectInspector) {
    columns(id).add(o, oi)
  }

  // We don't use these, but want to maintain Writable interface for SerDe
  override def write(out: DataOutput) {}

  override def readFields(in: DataInput) {}

  def close() {
    columns.foreach { _.close() }
  }
}

object ColumnarSerDe {

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
      case Category.PRIMITIVE => {
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
          case _ => throw new Exception("Invalid primitive object inspector category")
        }
      }
      case Category.LIST => {
        val listOISize = getFieldSize(oi.asInstanceOf[ListObjectInspector].getListElementObjectInspector())
        LIST_SIZE * listOISize
      }
      case Category.STRUCT => {
        val fieldRefs = oi.asInstanceOf[StructObjectInspector].getAllStructFieldRefs()
        fieldRefs.foldLeft(0)((sum, structField) =>
          sum + getFieldSize(structField.getFieldObjectInspector))
      }
      case Category.UNION => {
        val unionOIs = oi.asInstanceOf[UnionObjectInspector].getObjectInspectors()
        val unionOISizes = unionOIs.foldLeft(0)((sum, unionOI) =>
          sum + getFieldSize(unionOI))
        unionOISizes / unionOIs.size
      }
      case Category.MAP => {
        val mapOI = oi.asInstanceOf[MapObjectInspector]
        val keySize = getFieldSize(mapOI.getMapKeyObjectInspector())
        val valueSize = getFieldSize(mapOI.getMapValueObjectInspector())
        MAP_SIZE * (keySize + valueSize)
      }
      case _ => throw new Exception("Invalid primitive object inspector category")
    }
    return size
  }
}

class ColumnarStruct(val data: ColumnarWritable) {

  var row = -1

  def initializeNextRow() {
    row += 1
  }

  def getField(id: Int): Object = {
    data.getField(id, row)
  }

  def getFieldsAsList(): java.util.List[Object] = {
    val l = new java.util.ArrayList[Object]()
    for (i <- 0 until data.columns.length) {
      l.add(data.getField(i, row))
    }
    l
  }
}

class ColumnarStructObjectInspector(fields: java.util.List[StructField]) extends StructObjectInspector {

  override def getCategory(): Category =  {
    return Category.STRUCT
  }

  override def getTypeName(): String =  {
   return ObjectInspectorUtils.getStandardStructTypeName(this);
  }

  override def getStructFieldRef(fieldName: String): StructField = {
    return ObjectInspectorUtils.getStandardStructFieldRef(fieldName, fields)
  }

  override def getAllStructFieldRefs(): java.util.List[_ <: StructField] = {
    return fields
  }

  override def getStructFieldData(data: Object, fieldRef: StructField): Object = {
    data.asInstanceOf[ColumnarStruct].getField(
      fieldRef.asInstanceOf[IDStructField].getFieldID())
  }

  override def getStructFieldsDataAsList(data: Object): java.util.List[Object] = {
    if (data == null) {
      null
    } else {
      data.asInstanceOf[ColumnarStruct].getFieldsAsList()
    }
  }
}


object ColumnarStructObjectInspector {

  def apply(serDeParams: SerDeParameters): ColumnarStructObjectInspector = {
    val columnNames = serDeParams.getColumnNames()
    val columnTypes = serDeParams.getColumnTypes()
    val fields = new java.util.ArrayList[StructField]()
    for (i <- 0 until columnNames.size) {
      val typeInfo = columnTypes.get(i)
      val fieldOI = typeInfo.getCategory match {
        case Category.PRIMITIVE => SharkEnvSlave.objectInspectorLock.synchronized {
          PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(
            typeInfo.asInstanceOf[PrimitiveTypeInfo].getPrimitiveCategory)
        }
        case _ => SharkEnvSlave.objectInspectorLock.synchronized {
          LazyFactory.createLazyObjectInspector(
            typeInfo, serDeParams.getSeparators(), 1, serDeParams.getNullSequence(),
            serDeParams.isEscaped(), serDeParams.getEscapeChar())
        }
      }
      fields.add(new IDStructField(i, columnNames.get(i), fieldOI))
    }
    new ColumnarStructObjectInspector(fields)
  }
}


class IDStructField(
  val fieldID: Int, val fieldName: String, val fieldObjectInspector: ObjectInspector, val fieldComment: String)
extends StructField {

  def this(fieldID: Int, fieldName: String, fieldObjectInspector: ObjectInspector)
    = this(fieldID, fieldName, fieldObjectInspector, null)

  def getFieldID(): Int = {
    return fieldID
  }

  def getFieldName(): String =  {
    return fieldName
  }

  override def getFieldObjectInspector(): ObjectInspector = {
    return fieldObjectInspector
  }

  override def toString(): String =  {
    return "" + fieldID + ":" + fieldName
  }

  override def getFieldComment() : String = {
    return fieldComment
  }
}
