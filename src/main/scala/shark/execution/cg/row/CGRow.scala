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

package shark.execution.cg.row


import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.UnionObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory
import org.fusesource.scalate.TemplateEngine
import shark.execution.cg.CGAssertRuntimeException
import java.util.UUID
import scala.reflect.BeanProperty
import org.fusesource.scalate.Binding
import org.apache.hadoop.io.BytesWritable
import org.apache.hadoop.io.BooleanWritable
import org.apache.hadoop.hive.serde2.io.TimestampWritable
import org.apache.hadoop.hive.serde2.io.ShortWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.FloatWritable
import org.apache.hadoop.hive.serde2.io.DoubleWritable
import org.apache.hadoop.hive.serde2.io.DateWritable
import org.apache.hadoop.hive.serde2.io.ByteWritable
import scala.collection.mutable.ArrayBuffer
import com.esotericsoftware.kryo.Kryo
import java.io.ByteArrayOutputStream
import com.esotericsoftware.kryo.io.Output

object CGRowUtil {
  def extractPrimitiveClass(oi: PrimitiveObjectInspector) = {
    oi.getPrimitiveCategory() match {
      case PrimitiveCategory.BINARY => "byte[]"
      case PrimitiveCategory.BOOLEAN => "boolean"
      case PrimitiveCategory.BYTE =>"byte"
      case PrimitiveCategory.DATE =>"java.util.Date"
      case PrimitiveCategory.DOUBLE =>"double"
      case PrimitiveCategory.FLOAT =>"float"
      case PrimitiveCategory.INT =>"int"
      case PrimitiveCategory.LONG =>"long"
      case PrimitiveCategory.SHORT =>"short"
      case PrimitiveCategory.STRING =>"String"
      case PrimitiveCategory.TIMESTAMP =>"java.sql.Timestamp"
      case _ => throw new CGAssertRuntimeException("couldn't find the primitive category")
    }
  }
  
  def extractBoxedPrimitiveClass(oi: PrimitiveObjectInspector) = {
    oi.getPrimitiveCategory() match {
      case PrimitiveCategory.BINARY => "byte[]"
      case PrimitiveCategory.BOOLEAN => "Boolean"
      case PrimitiveCategory.BYTE =>"Byte"
      case PrimitiveCategory.DATE =>"java.util.Date"
      case PrimitiveCategory.DOUBLE =>"Double"
      case PrimitiveCategory.FLOAT =>"Float"
      case PrimitiveCategory.INT =>"Integer"
      case PrimitiveCategory.LONG =>"Long"
      case PrimitiveCategory.SHORT =>"Short"
      case PrimitiveCategory.STRING =>"String"
      case PrimitiveCategory.TIMESTAMP =>"java.sql.Timestamp"
      case _ => throw new CGAssertRuntimeException("couldn't find the primitive category")
    }
  }
  
  def extractHashMapClass(key: CGField[_], value: CGField[_]) = "java.util.HashMap<%s,%s>".format(key.clazz, value.clazz)
  
  def extractListClass(value: CGField[_]) = "java.util.ArrayList<%s>".format(value.clazz)
    
  def extractUnionClass() = randomClassName()
  
  def randomClassName() = "GEN" + UUID.randomUUID().toString().replaceAll("\\-", "_")
  
  def unionName(name: String, tag: Int) = "%s_%d".format(name, tag)
  
  def serialize(o: Object): Array[Byte] = {
    var array = new ByteArrayOutputStream()
    var output= new Output(array)
    new Kryo().writeObject(output, o)

    output.close()
    array.close()
    
    array.toByteArray()
  }
}

abstract class CGField[+T<:ObjectInspector](val oi: T, 
    val name: String, 
    val clazz: String, 
    val primitive: String) {
  val maskName = "MARK_%s".format(name)
  var constantNull = false
  var constant = oi.isInstanceOf[ConstantObjectInspector] 
  
  def this(oi: T, name: String, clz: String) {
    this(oi, name, clz, clz)
  }
  
  //def the static classes / blocks / methods
  def defStaticBlocks(): String = null

  def defField() = CGRow.layout(CGRow.CG_ROW_FIELD, Map("field" -> this))
  
  // field de-serialization in the KryoSerializable.read
  def defRead():String = readString(name)
  // field serialization in the KryoSerializable.write
  def defWrite(): String = writeString(name)
  
  // de-serialization in the KryoSerializable.read 
  def readString(name: String): String
  // serialization in the KryoSerializable.write
  def writeString(name: String): String
  
  // factory method to create instances of (struct / union)
  def getValue(oiName: String, dataName: String): String = "%s.BUILD_%s()".format(clazz, name)
  
  // used by CGStruct, which is just code snippet for loading the non-constant field value from ObjectInspector/data
  def loadPiece(): String = null
  
  // used by CGStruct / CGUnion, which initializing the constant field (Map/List only)
  def fieldValue() = "INITIAL_%s()".format(name)
  
  def isConstantNull(): Boolean = constantNull
  def markValidity(valid: Boolean) = "mask.set(%s, %s)".format(maskName, valid)
}

case class CGPrimitive(_oi: PrimitiveObjectInspector, _name: String) 
  extends CGField[PrimitiveObjectInspector](_oi, _name, CGRowUtil.extractBoxedPrimitiveClass(_oi), CGRowUtil.extractPrimitiveClass(_oi)) {
  override def readString(name: String): String = {
      oi.getPrimitiveCategory() match {
      case PrimitiveCategory.BINARY => "%s = input.readBytes(input.read());".format(name)
      case PrimitiveCategory.BOOLEAN =>"%s = input.readBoolean();".format(name)
      case PrimitiveCategory.BYTE =>"%s = input.readByte();".format(name)
      case PrimitiveCategory.DATE =>"%s = new java.util.Date(input.readLong());".format(name)
      case PrimitiveCategory.DOUBLE =>"%s = input.readDouble();".format(name)
      case PrimitiveCategory.FLOAT =>"%s = input.readFloat();".format(name)
      case PrimitiveCategory.INT =>"%s = input.readInt();".format(name)
      case PrimitiveCategory.LONG =>"%s = input.readLong();".format(name)
      case PrimitiveCategory.SHORT =>"%s = input.readShort();".format(name)
      case PrimitiveCategory.STRING =>"%s = input.readString();".format(name)
      case PrimitiveCategory.TIMESTAMP =>"%s = new java.sql.Timestamp(input.readLong()); %s.setNanos(input.readInt());".format(name, name)
      case _ => throw new CGAssertRuntimeException("couldn't find the primitive category")
    }
  }
  
  override def writeString(name: String): String = {
        oi.getPrimitiveCategory() match {
      case PrimitiveCategory.BINARY => "output.writeInt(%s.length); output.write(%s);".format(name, name)
      case PrimitiveCategory.BOOLEAN =>"output.writeBoolean(%s);".format(name)
      case PrimitiveCategory.BYTE =>"output.writeByte(%s);".format(name)
      case PrimitiveCategory.DATE =>"output.writeLong(%s.getTime());".format(name)
      case PrimitiveCategory.DOUBLE =>"output.writeDouble(%s);".format(name)
      case PrimitiveCategory.FLOAT =>"output.writeFloat(%s);".format(name)
      case PrimitiveCategory.INT =>"output.writeInt(%s);".format(name)
      case PrimitiveCategory.LONG =>"output.writeLong(%s);".format(name)
      case PrimitiveCategory.SHORT =>"output.writeShort(%s);".format(name)
      case PrimitiveCategory.STRING =>"output.writeString(%s);".format(name)
      case PrimitiveCategory.TIMESTAMP =>"output.writeLong(%s.getTime()); output.writeInt(%s.getNanos());".format(name, name)
      case _ => throw new CGAssertRuntimeException("couldn't find the primitive category")
    }
  }
  
  override def getValue(oiName: String, dataName: String): String = 
    CGRow.layout(
        CGRow.CG_ROW_OI_2_PRIMITIVE, 
        Map("obj"->this, 
            "varoi"->oiName, 
            "varname"->dataName))
            
  override def loadPiece(): String = CGRow.layout(CGRow.CG_ROW_LOAD_PIECE_PRIMITIVE, Map("obj"->this))
}

case class CGMap(_oi: MapObjectInspector, _name: String, val key: CGField[_<:ObjectInspector], val value: CGField[_<:ObjectInspector]) 
  extends CGField[MapObjectInspector](_oi, _name, CGRowUtil.extractHashMapClass(key, value)) {
  lazy val bytes: String = 
    if (constant) 
      CGRowUtil.serialize(oi.asInstanceOf[ConstantObjectInspector].getWritableConstantValue()) 
    else 
      null
  
  override def defStaticBlocks(): String = CGRow.generate(this)
  
  override def readString(name: String): String = {
    "%s = kryo.readObject(input, java.util.HashMap.class);".format(name)
  }
  
  override def writeString(name: String): String = {
    "kryo.writeObjectOrNull(output, %s, java.util.HashMap.class);".format(name)
  }

  override def getValue(oiName: String, dataName: String): String = "BUILD_%s(%s, %s)".format(name, oiName, dataName)

  override def loadPiece(): String = CGRow.layout(CGRow.CG_ROW_LOAD_PIECE_MAP, Map("obj"->this))
}

case class CGStruct(_oi: StructObjectInspector, _name: String, val fields: Array[CGField[_]]) 
  extends CGField(_oi, _name, CGRowUtil.randomClassName()) {

  override def readString(name: String): String = {
    "%s = kryo.readObject(input, %s.class);".format(name, clazz)
  }
  override def writeString(name: String): String = {
    "kryo.writeObjectOrNull(output, %s, %s.class);".format(name, clazz)
  }
  override def defStaticBlocks(): String = CGRow.generate(this)
  
  override def getValue(oiName: String, dataName: String): String = "%s.BUILD(%s, %s)".format(clazz, oiName, dataName)
  override def loadPiece(): String = CGRow.layout(CGRow.CG_ROW_LOAD_PIECE_STRUCT, Map("obj"->this))
}

case class CGList(_oi: ListObjectInspector, _name: String, val field: CGField[_<:ObjectInspector]) 
  extends CGField(_oi, _name, CGRowUtil.extractListClass(field)) {
  lazy val bytes: String = 
    if (constant) 
      CGRowUtil.serialize(oi.asInstanceOf[ConstantObjectInspector].getWritableConstantValue()) 
    else 
      null
  
  override def defStaticBlocks(): String = CGRow.generate(this)

  override def readString(name: String): String = {
    if (field.isInstanceOf[CGStruct]) {
      field.readString(name)
    } else {
      "%s = kryo.readObject(input, java.util.ArrayList.class);".format(name)
    }
  }
  
  override def writeString(name: String): String = {
    if (field.isInstanceOf[CGStruct]) {
      field.writeString(name)
    } else {
      "kryo.writeObjectOrNull(output, %s, java.util.ArrayList.class);".format(name)
    }
  }

  override def getValue(oiName: String, dataName: String): String = "BUILD_%s(%s, %s)".format(name, oiName, dataName)

  override def loadPiece(): String = CGRow.layout(CGRow.CG_ROW_LOAD_PIECE_LIST, Map("obj"->this))
}

case class CGUnion(_oi:UnionObjectInspector, _name: String, val fields: Array[(Int, CGField[_])])
  extends CGField(_oi, _name, CGRowUtil.extractUnionClass()) {
  
  def unionName(tag: Int) = CGRowUtil.unionName(name, tag)
  
  override def readString(name: String): String = {
    "%s = kryo.readObject(input, %s.class);".format(name, clazz)
  }
  
  override def writeString(name: String): String = {
    "kryo.writeObjectOrNull(output, %s, %s.class);".format(name, clazz)
  }
  
  override def defStaticBlocks(): String = CGRow.generate(this)
  
  override def getValue(oiName: String, dataName: String): String = "%s.BUILD(%s, %s)".format(clazz, oiName, dataName)
  
//  override def readString(name: String): String = {
//    var block = fields.map{
//      case (tag, field) => "if(%s == %d) {%s;}".format(name, tag, field.readString(unionName(tag)))
//    }.reduce(_ + " else " + _)
//
//    new StringBuffer("%s = input.read(); %s".format(name, block)).toString()
//  }
//  override def writeString(name: String): String = {
//    var block = fields.map{
//      case (tag, field) => "if(%s == %d) {%s;}".format(name, tag, field.writeString(unionName(tag)))
//    }.reduce(_ + " else " + _)
//
//    new StringBuffer("output.writeInt(%s); %s".format(name, block)).toString()
//  }
  override def loadPiece(): String = CGRow.layout(CGRow.CG_ROW_LOAD_PIECE_UNION, Map("obj"->this))
}

class CGRowContext(
    var className: String, 
    var fieldTypes: Array[CGField[_]], 
    var isOutter: Boolean = false, 
    var packageName: String = null) {
}

object CGRow {
  val CG_ROW_CLASS_STRUCT = "shark/execution/cg/row/cg_class_struct.ssp"
  val CG_ROW_CLASS_UNION  = "shark/execution/cg/row/cg_class_union.ssp"
  val CG_ROW_OI_2_LIST = "shark/execution/cg/row/cg_oi_2_list.ssp"
  val CG_ROW_OI_2_MAP  = "shark/execution/cg/row/cg_oi_2_map.ssp"
  val CG_ROW_OI_2_PRIMITIVE = "shark/execution/cg/row/cg_oi_2_primitive.ssp"
  val CG_ROW_FIELD = "shark/execution/cg/row/cgfield.ssp"
  val CG_ROW_INITIAL_LIST = "shark/execution/cg/row/cginitial_list.ssp"
  val CG_ROW_INITIAL_MAP = "shark/execution/cg/row/cginitial_map.ssp"
  val CG_ROW_LOAD_PIECE_LIST = "shark/execution/cg/row/cgload_piece_list.ssp"
  val CG_ROW_LOAD_PIECE_MAP = "shark/execution/cg/row/cgload_piece_map.ssp"
  val CG_ROW_LOAD_PIECE_PRIMITIVE = "shark/execution/cg/row/cgload_piece_primitive.ssp"
  val CG_ROW_LOAD_PIECE_STRUCT = "shark/execution/cg/row/cgload_piece_struct.ssp"
  val CG_ROW_LOAD_PIECE_UNION = "shark/execution/cg/row/cgload_piece_union.ssp"
    
  val engine = new TemplateEngine
  engine.allowReload  = false
  engine.allowCaching = true

  def layout(ssp: String, map: Map[String, Any]) = {
    engine.layout(ssp, map)
  }
  
  def generate(struct: CGStruct, isOutter: Boolean = false, packageName: String = null): String = {
    var obj = new CGRowContext(struct.clazz, struct.fields, isOutter, packageName)
    
    CGRow.layout(CGRow.CG_ROW_CLASS_STRUCT, Map("obj" -> obj))
  }
  
  def generate(union: CGUnion): String = {
    CGRow.layout(CGRow.CG_ROW_CLASS_UNION, Map("obj" -> union))
  }
  
  def generate(list: CGList): String = {
    CGRow.layout(CGRow.CG_ROW_OI_2_LIST, Map("obj" -> list))
  }
  
  def generate(map: CGMap): String = {
    CGRow.layout(CGRow.CG_ROW_OI_2_MAP, Map("obj" -> map))
  }
  
  def generate(oi: StructObjectInspector): String = {
    var struct = initial(oi, null).asInstanceOf[CGStruct]
    
    generate(struct, true, "shark.execution.cg.row")
  }
  
  def initial(oi: ObjectInspector, name: String): CGField[_<:ObjectInspector] = {
    import collection.JavaConversions._
    oi match {
      case a: StructObjectInspector=> 
        CGStruct(a,
            name,
            a.getAllStructFieldRefs().map(f => {initial(f.getFieldObjectInspector(), f.getFieldName())}).toArray
            )
      case a: ListObjectInspector=>
        CGList(a,
            name,
            initial(a.getListElementObjectInspector(), "%s_l".format(name)) // default name
            )
      case a: MapObjectInspector=>
        CGMap(a, 
            name,
              initial(a.getMapKeyObjectInspector(), "%s_k".format(name)), // default name
              initial(a.getMapValueObjectInspector(), "%s_v".format(name)) // default name
              )
      case a: PrimitiveObjectInspector=> CGPrimitive(a, name)
      case a: UnionObjectInspector=>
        CGUnion(a,name,
            a.getObjectInspectors().zipWithIndex.map(f=>(f._2, initial(f._1, CGRowUtil.unionName(name, f._2)))).toArray 
            )
    }
  }
}