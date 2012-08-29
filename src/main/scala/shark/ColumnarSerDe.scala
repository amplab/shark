package shark

import java.io.DataOutput
import java.io.DataInput
import java.util.Properties

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Writable
import org.apache.hadoop.hive.serde2.{ByteStream, SerDe, SerDeException}
import org.apache.hadoop.hive.serde2.`lazy`.{LazyFactory, LazySimpleSerDe}
import org.apache.hadoop.hive.serde2.`lazy`.LazySimpleSerDe.SerDeParameters
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils
import org.apache.hadoop.hive.serde2.objectinspector.StructField
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory
import org.apache.hadoop.hive.serde2.typeinfo.{TypeInfo, PrimitiveTypeInfo}


class ColumnarSerDe extends SerDe {

  var cachedObjectInspector: StructObjectInspector = _
  var cachedWritable: ColumnarWritable = _
  var cachedStruct: ColumnarStruct = _
  var serDeParams: SerDeParameters = _
  val serializeStream = new ByteStream.Output()

  def initialize(job: Configuration, tbl: Properties) {
    serDeParams = LazySimpleSerDe.initSerdeParams(job, tbl, getClass().getName())
    // create oi & writable
    cachedObjectInspector = ColumnarStructObjectInspector(serDeParams)
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
      cachedWritable = new ColumnarWritable(cachedObjectInspector)
    }
    val soi = objInspector.asInstanceOf[StructObjectInspector]
    val fields = soi.getAllStructFieldRefs
    //Doesn't handle complex things yet
    var i = 0
    while (i < fields.size) {
    //for (i <- 0 until fields.size) {
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


class ColumnarWritable(oi: StructObjectInspector) extends Writable {

  val fields = oi.getAllStructFieldRefs
  val columns = new Array[Column](fields.size)

  (0 until columns.length).foreach { i =>
    columns(i) = Column(fields.get(i).getFieldObjectInspector)
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

