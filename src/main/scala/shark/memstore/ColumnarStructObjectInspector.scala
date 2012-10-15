package shark.memstore

import java.util.{ArrayList => JArrayList, List => JList}

import org.apache.hadoop.hive.serde2.`lazy`.LazyFactory
import org.apache.hadoop.hive.serde2.`lazy`.LazySimpleSerDe.SerDeParameters
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector, ObjectInspectorUtils,
  StructField, StructObjectInspector}
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo

import shark.{SharkConfVars, SharkEnvSlave}


class ColumnarStructObjectInspector(fields: JList[StructField]) extends StructObjectInspector {

  override def getCategory: Category = Category.STRUCT

  override def getTypeName: String = ObjectInspectorUtils.getStandardStructTypeName(this)

  override def getStructFieldRef(fieldName: String): StructField =
    ObjectInspectorUtils.getStandardStructFieldRef(fieldName, fields)

  override def getAllStructFieldRefs: JList[_ <: StructField] = fields

  override def getStructFieldData(data: Object, fieldRef: StructField): Object =
    data.asInstanceOf[ColumnarStruct].getField(
        fieldRef.asInstanceOf[ColumnarStructObjectInspector.IDStructField].fieldID)

  override def getStructFieldsDataAsList(data: Object): JList[Object] =
    if (data == null) null else data.asInstanceOf[ColumnarStruct].getFieldsAsList()
}


object ColumnarStructObjectInspector {

  def apply(serDeParams: SerDeParameters): ColumnarStructObjectInspector = {

    val columnNames = serDeParams.getColumnNames()
    val columnTypes = serDeParams.getColumnTypes()
    val fields = new JArrayList[StructField]()
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

  class IDStructField(
      val fieldID: Int,
      val fieldName: String,
      val fieldObjectInspector: ObjectInspector,
      val fieldComment: String)
    extends StructField {

    def this(fieldID: Int, fieldName: String, fieldObjectInspector: ObjectInspector) =
      this(fieldID, fieldName, fieldObjectInspector, null)

    override def getFieldName: String = fieldName
    override def getFieldObjectInspector: ObjectInspector = fieldObjectInspector
    override def toString(): String = "" + fieldID + ":" + fieldName
    override def getFieldComment() : String = fieldComment
  }
}

