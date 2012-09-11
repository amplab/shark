package shark

import java.io.{DataInput, DataOutput}
import java.util.Properties

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hdfs.DFSConfigKeys
import org.apache.hadoop.hive.serde2.{ByteStream, SerDe, SerDeException}
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


class ColumnarSerDe extends SerDe {

  var cachedObjectInspector: StructObjectInspector = _
  var cachedWritable: ColumnarWritable = _
  var cachedStruct: ColumnarStruct = _
  var conf: Configuration = _
  var serDeParams: SerDeParameters = _
  val serializeStream = new ByteStream.Output()

  def initialize(job: Configuration, tbl: Properties) {
    serDeParams = LazySimpleSerDe.initSerdeParams(job, tbl, getClass().getName())
    // Create oi & writable.
    cachedObjectInspector = ColumnarStructObjectInspector(serDeParams)
    conf = job
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
  
  def serialize(obj: Object, objInspector: ObjectInspector): Writable = {
    if (cachedWritable == null) {
      cachedWritable = new ColumnarWritable(cachedObjectInspector, conf)
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

class ColumnarWritable(oi: StructObjectInspector, conf: Configuration)
  extends Writable {

  val fields = oi.getAllStructFieldRefs
  val columns = new Array[Column](fields.size)

  // Determine the initial capacity for ArrayList columns.
  var initialColumnSize = SharkConfVars.getIntVar(conf, SharkConfVars.COLUMN_INITIALSIZE)
  if (initialColumnSize == - 1) {
    // Try both "dfs.block.size" and "dfs.blocksize" (from DFS_BLOCK_SIZE_KEY),
    // which is the new name since Hadoop 0.21.0.
    val partitionSize = conf.getLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY,
      conf.getLong("dfs.block.size", DFSConfigKeys.DFS_BLOCK_SIZE_DEFAULT))
    val fieldSize = ColumnarWritable.getFieldSize(oi)
    initialColumnSize = (partitionSize / (fieldSize *
      oi.getAllStructFieldRefs().size).toLong).toInt
  }

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

object ColumnarWritable {

  // Sizes of primitive types
  private val BOOLEAN_SIZE = 1
  private val BYTE_SIZE = 1
  private val SHORT_SIZE = 2
  private val INT_SIZE = 4
  private val LONG_SIZE = 8
  private val FLOAT_SIZE = 4
  private val DOUBLE_SIZE = 8
  private val STRING_SIZE = 16

  // Void columns are represented by NullWritable, which is a singleton, so it's not
  // large enough to matter.
  private val NULL_SIZE = 0

  // Size of an object reference.
  private val pointerSize = if (System.getProperty("os.arch").contains("64")) 8 else 4

  // Estimates for complex types. Includes overhead and pointer sizes for Java HashMap and
  // ArrayList objects, based on Spark's SizeEstimator.estimate().
  private val avgListLength = 5
  private val listOverhead = 88 + pointerSize * avgListLength
  private val avgMapSize = 5
  private val mapOverhead = 152 + pointerSize * avgMapSize

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
        listOverhead + listOISize * avgListLength
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
        mapOverhead + (avgMapSize * (keySize + valueSize))
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
        case Category.PRIMITIVE => SharkEnv.objectInspectorLock.synchronized {
          PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(
            typeInfo.asInstanceOf[PrimitiveTypeInfo].getPrimitiveCategory)
        }
        case _ => SharkEnv.objectInspectorLock.synchronized {
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
  val fieldID: Int, val fieldName: String, val fieldObjectInspector: ObjectInspector)
extends StructField {

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
}

