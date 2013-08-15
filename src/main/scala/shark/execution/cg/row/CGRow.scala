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
      case PrimitiveCategory.DATE =>"java.sql.Date"
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
      case PrimitiveCategory.DATE =>"java.sql.Date"
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
    
  var u: Int = 0
  def extractUnionClass() = if(u==0) {u=1; "UUnion"} else {u+=1;"UUnion%d".format(u)} // randomClassName()
  
  var s: Int = 0
  def randomClassName() = if(s==0) {s=1; "SStruct"} else {s+=1;"SStruct%d".format(s)} // "GEN" + UUID.randomUUID().toString().replaceAll("\\-", "_")
  
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
  
  var parent: CGField[_] = _
  def parentFullClass(): String = if (parent != null) parent.cgClassName() else null
  def cgClassName(): String = throw new CGAssertRuntimeException("ONLY the CGStruct / CGUnion has the full class name")

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
  def getValue(oi: String, data: String): String = "%s.BUILD_%s()".format(clazz, name)
  
  // used by CGStruct, which is just code snippet for loading the non-constant field value from ObjectInspector/data
  def loadPiece(): String = null
  
  // used by CGStruct / CGUnion, which initializing the constant field (Map/List only)
  def fieldValue() = "INITIAL_%s()".format(name)
  
  def isConstantNull(): Boolean = constantNull
  def markValidity(valid: Boolean) = "mask.set(%s, %s)".format(maskName, valid)
  
  
  // the following methods are used for ObjectInspector generation
  def oiClassName(): String = throw new CGAssertRuntimeException("")
  def createOI(): String
  def writableClass(): String
  def java2Writable(javaVarName: String, writableVarName: String): (String, ()=>String) // (code snippet for the conversion, code block (functions) for extra)
  def defStructField(): (String, String, String) // (field name, class name, struct field class definition)
  def oiClass(): String
}

class CGPrimitive(oi: PrimitiveObjectInspector, name: String) 
  extends CGField[PrimitiveObjectInspector](oi, name, CGRowUtil.extractBoxedPrimitiveClass(oi), CGRowUtil.extractPrimitiveClass(oi)) {
  override def readString(name: String): String = {
      oi.getPrimitiveCategory() match {
      case PrimitiveCategory.BINARY => "%s = input.readBytes(input.read());".format(name)
      case PrimitiveCategory.BOOLEAN =>"%s = input.readBoolean();".format(name)
      case PrimitiveCategory.BYTE =>"%s = input.readByte();".format(name)
      case PrimitiveCategory.DATE =>"%s = new java.sql.Date(input.readLong());".format(name)
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
  
  override def getValue(oi: String, data: String): String = 
    CGRow.layout(
        CGRow.CG_ROW_OI_2_PRIMITIVE, 
        Map("obj"->this, 
            "varoi"->oi, 
            "varname"->data))
            
  override def loadPiece(): String = CGRow.layout(CGRow.CG_ROW_LOAD_PIECE_PRIMITIVE, Map("obj"->this))

  // the following methods are used for ObjectInspector generation
  override def createOI(): String = oi.getPrimitiveCategory() match {
    case PrimitiveCategory.BINARY    => "TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(TypeInfoFactory.binaryTypeInfo)"
    case PrimitiveCategory.BOOLEAN   => "TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(TypeInfoFactory.booleanTypeInfo)"
    case PrimitiveCategory.BYTE      => "TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(TypeInfoFactory.byteTypeInfo)"
    case PrimitiveCategory.DATE      => "TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(TypeInfoFactory.dateTypeInfo)"
    case PrimitiveCategory.DOUBLE    => "TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(TypeInfoFactory.doubleTypeInfo)"
    case PrimitiveCategory.FLOAT     => "TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(TypeInfoFactory.floatTypeInfo)"
    case PrimitiveCategory.INT       => "TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(TypeInfoFactory.intTypeInfo)"
    case PrimitiveCategory.LONG      => "TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(TypeInfoFactory.longTypeInfo)"
    case PrimitiveCategory.SHORT     => "TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(TypeInfoFactory.shortTypeInfo)"
    case PrimitiveCategory.STRING    => "TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(TypeInfoFactory.stringTypeInfo)"
    case PrimitiveCategory.TIMESTAMP => "TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(TypeInfoFactory.timestampTypeInfo)"
    case _                           => throw new CGAssertRuntimeException("couldn't find the primitive category")
  }
  override def writableClass(): String = oi.getPrimitiveCategory() match {
    case PrimitiveCategory.BINARY    => "org.apache.hadoop.io.BytesWritable"
    case PrimitiveCategory.BOOLEAN   => "org.apache.hadoop.io.BooleanWritable"
    case PrimitiveCategory.BYTE      => "org.apache.hadoop.hive.serde2.io.ByteWritable"
    case PrimitiveCategory.DATE      => "org.apache.hadoop.hive.serde2.io.DateWritable"
    case PrimitiveCategory.DOUBLE    => "org.apache.hadoop.hive.serde2.io.DoubleWritable"
    case PrimitiveCategory.FLOAT     => "org.apache.hadoop.io.FloatWritable"
    case PrimitiveCategory.INT       => "org.apache.hadoop.io.IntWritable"
    case PrimitiveCategory.LONG      => "org.apache.hadoop.io.LongWritable"
    case PrimitiveCategory.SHORT     => "org.apache.hadoop.hive.serde2.io.ShortWritable"
    case PrimitiveCategory.STRING    => "org.apache.hadoop.io.Text"
    case PrimitiveCategory.TIMESTAMP => "org.apache.hadoop.hive.serde2.io.TimestampWritable"
    case _                           => throw new CGAssertRuntimeException("couldn't find the primitive category")
  }
  
  override def java2Writable(javaVarName: String, writableVarName: String) = 
    if (writableVarName == null)
      oi.getPrimitiveCategory() match {
        case PrimitiveCategory.BINARY    => ("new org.apache.hadoop.io.BytesWritable(%s)".format(javaVarName), () => null)
        case PrimitiveCategory.BOOLEAN   => ("new org.apache.hadoop.io.BooleanWritable(%s)".format(javaVarName), () => null)
        case PrimitiveCategory.BYTE      => ("new org.apache.hadoop.hive.serde2.io.ByteWritable(%s)".format(javaVarName), () => null)
        case PrimitiveCategory.DATE      => ("new org.apache.hadoop.hive.serde2.io.DateWritable(%s)".format(javaVarName), () => null)
        case PrimitiveCategory.DOUBLE    => ("new org.apache.hadoop.hive.serde2.io.DoubleWritable(%s)".format(javaVarName), () => null)
        case PrimitiveCategory.FLOAT     => ("new org.apache.hadoop.io.FloatWritable(%s)".format(javaVarName), () => null)
        case PrimitiveCategory.INT       => ("new org.apache.hadoop.io.IntWritable(%s)".format(javaVarName), () => null)
        case PrimitiveCategory.LONG      => ("new org.apache.hadoop.io.LongWritable(%s)".format(javaVarName), () => null)
        case PrimitiveCategory.SHORT     => ("new org.apache.hadoop.hive.serde2.io.ShortWritable(%s)".format(javaVarName), () => null)
        case PrimitiveCategory.STRING    => ("new org.apache.hadoop.io.Text(%s)".format(javaVarName), () => null)
        case PrimitiveCategory.TIMESTAMP => ("new org.apache.hadoop.hive.serde2.io.TimestampWritable(%s)".format(javaVarName), () => null)
        case _                           => throw new CGAssertRuntimeException("couldn't find the primitive category")
      }
    else
      oi.getPrimitiveCategory() match {
        case PrimitiveCategory.BINARY    => ("%s.set(%s, 0, %s.length);".format(writableVarName, javaVarName, javaVarName), () => null)
        case PrimitiveCategory.BOOLEAN   => ("%s.set(%s);".format(writableVarName, javaVarName), ()=>null)
        case PrimitiveCategory.BYTE      => ("%s.set(%s);".format(writableVarName, javaVarName), ()=>null)
        case PrimitiveCategory.DATE      => ("%s.set(%s);".format(writableVarName, javaVarName), ()=>null)
        case PrimitiveCategory.DOUBLE    => ("%s.set(%s);".format(writableVarName, javaVarName), ()=>null)
        case PrimitiveCategory.FLOAT     => ("%s.set(%s);".format(writableVarName, javaVarName), ()=>null)
        case PrimitiveCategory.INT       => ("%s.set(%s);".format(writableVarName, javaVarName), ()=>null)
        case PrimitiveCategory.LONG      => ("%s.set(%s);".format(writableVarName, javaVarName), ()=>null)
        case PrimitiveCategory.SHORT     => ("%s.set(%s);".format(writableVarName, javaVarName), ()=>null)
        case PrimitiveCategory.STRING    => ("%s.set(%s);".format(writableVarName, javaVarName), ()=>null)
        case PrimitiveCategory.TIMESTAMP => ("%s.set(%s);".format(writableVarName, javaVarName), ()=>null)
        case _                           => throw new CGAssertRuntimeException("couldn't find the primitive category")
      }
      
  
  override def defStructField(): (String, String, String) = {
    var classname = "CGStructField%s".format(name)
    (name, classname, CGRow.layout(CGRow.CG_SF_PRIMITIVE, Map("obj"->this, "classname"->classname)))
  }
  
  override def oiClass(): String = null
}

class CGMap(oi: MapObjectInspector, name: String, val key: CGField[_<:ObjectInspector], val value: CGField[_<:ObjectInspector]) 
  extends CGField[MapObjectInspector](oi, name, CGRowUtil.extractHashMapClass(key, value)) {
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

  override def getValue(oi: String, data: String): String = "BUILD_%s(%s, %s)".format(name, oi, data)

  override def loadPiece(): String = CGRow.layout(CGRow.CG_ROW_LOAD_PIECE_MAP, Map("obj"->this))
  
  // the following methods are used for ObjectInspector generation
  override def createOI(): String = "ObjectInspectorFactory.getStandardMapObjectInspector(%s,%s)".format(key.createOI(), value.createOI())
  override def writableClass(): String = "java.util.HashMap<%s, %s>".format(key.writableClass(), value.writableClass())
    
  override def java2Writable(javaVarName: String, writableVarName: String) = {
    var methodName = "%s_%s_transform".format(name, javaVarName)
    (if (writableVarName!= null) 
      "%s=%s(%s);".format(writableVarName, methodName, javaVarName)
     else
      "%s(%s)".format(methodName, javaVarName), ()=>{
      CGRow.layout(CGRow.CG_OI_TRANSFORM_MAP, 
        Map("obj" -> this, "methodName" -> methodName))
    })
  }
  
  override def defStructField(): (String, String, String) = {
    var classname = "CGStructField%s".format(name)
    (name, classname, CGRow.layout(CGRow.CG_SF_MAP, Map("obj"->this, "classname"->classname)))
  }
  
  override def oiClass(): String = null
}

class CGList(oi: ListObjectInspector, name: String, val field: CGField[_<:ObjectInspector]) 
  extends CGField(oi, name, CGRowUtil.extractListClass(field)) {
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

  override def getValue(oi: String, data: String): String = "BUILD_%s(%s, %s)".format(name, oi, data)

  override def loadPiece(): String = CGRow.layout(CGRow.CG_ROW_LOAD_PIECE_LIST, Map("obj"->this))
  // the following methods are used for ObjectInspector generation
  override def createOI(): String = "ObjectInspectorFactory.getStandardListObjectInspector(%s)".format(field.createOI())
  
  override def writableClass(): String = "java.util.ArrayList<%s>".format(field.writableClass())
      
  override def java2Writable(javaVarName: String, writableVarName: String) = {
    var methodName = if(javaVarName == null) 
                        "%s_transform".format(name)
                     else
                        "%s_%s_transform".format(name, javaVarName)
    (if (writableVarName!=null)
      "%s = %s(%s);".format(writableVarName, methodName, javaVarName)
     else
      "%s(%s)".format(methodName, javaVarName), ()=>{
      CGRow.layout(CGRow.CG_OI_TRANSFORM_LIST, 
        Map("obj" -> this, "methodName" -> methodName))
    })
  }
  
  override def defStructField(): (String, String, String) = {
    var classname = "CGStructField%s".format(name)
    (name, classname, CGRow.layout(CGRow.CG_SF_LIST, Map("obj"->this, "classname"->classname)))
  }
  
  override def oiClass(): String = null
}

class CGStruct(oi: StructObjectInspector, name: String, val fields: Array[CGField[_]]) 
  extends CGField(oi, name, CGRowUtil.randomClassName()) {

  var packageName: String = _
  
  { fields.foreach(_.parent=this) }
  
  override def readString(name: String): String = {
    "%s = kryo.readObject(input, %s.class);".format(name, clazz)
  }
  override def writeString(name: String): String = {
    "kryo.writeObjectOrNull(output, %s, %s.class);".format(name, clazz)
  }
  override def defStaticBlocks(): String = CGRow.generate(this)
  
  override def getValue(oi: String, data: String): String = "%s.BUILD(%s, %s)".format(clazz, oi, data)
  override def loadPiece(): String = CGRow.layout(CGRow.CG_ROW_LOAD_PIECE_STRUCT, Map("obj"->this))
  
  // the following methods are used for ObjectInspector generation
  override def oiClassName(): String = "%sStructObjectInspector".format(clazz)
  override def createOI(): String = "new %s()".format(oiClassName())
  override def writableClass(): String = cgClassName()
  override def java2Writable(javaVarName: String, writableVarName: String) = 
    (if(writableVarName!=null)
      "%s=%s;".format(writableVarName, javaVarName)
     else
       javaVarName, ()=>null)
  override def defStructField(): (String, String, String) = {
    var classname = "CGStructField%s".format(name)
    (name, classname, CGRow.layout(CGRow.CG_SF_STRUCT, Map("obj"->this, "classname"->classname)))
  }
  
  override def oiClass(): String = {CGRow.layout(CGRow.CG_OI_STRUCT, Map("struct"->this))}
  
  override def cgClassName(): String = {
    var parentClass = parentFullClass()
    if (null == parentClass) {
      if (packageName != null) "%s.%s".format(packageName, clazz) else clazz
    } else {
      "%s.%s".format(parentClass, clazz)
    }
  }
}

class CGUnion(oi:UnionObjectInspector, name: String, val fields: Array[(Int, CGField[_])])
  extends CGField(oi, name, CGRowUtil.extractUnionClass()) {
  
  { fields.foreach(f=>f._2.parent=this) }
  
  def unionName(tag: Int) = CGRowUtil.unionName(name, tag)
  
  override def readString(name: String): String = {
    "%s = kryo.readObject(input, %s.class);".format(name, clazz)
  }
  
  override def writeString(name: String): String = {
    "kryo.writeObjectOrNull(output, %s, %s.class);".format(name, clazz)
  }
  
  override def defStaticBlocks(): String = CGRow.generate(this)
  
  override def getValue(oi: String, data: String): String = "%s.BUILD(%s, %s)".format(clazz, oi, data)

  override def loadPiece(): String = CGRow.layout(CGRow.CG_ROW_LOAD_PIECE_UNION, Map("obj"->this))
  
  // the following methods are used for ObjectInspector generation
  override def oiClassName(): String = "%sUnionObjectInspector".format(clazz)
  override def createOI(): String = "new %s()".format(oiClassName())
  override def writableClass(): String = cgClassName()
  override def java2Writable(javaVarName: String, writableVarName: String) = 
    (if(writableVarName!=null)
      "%s=%s;".format(writableVarName, javaVarName)
     else
       javaVarName, ()=>null)
  override def defStructField(): (String, String, String) = {
    var classname = "CGStructField%s".format(name)
    (name, classname, CGRow.layout(CGRow.CG_SF_UNION, Map("obj"->this, "classname"->classname)))
  }  
  override def oiClass(): String = {CGRow.layout(CGRow.CG_OI_UNION, Map("obj"->this))}
  
  override def cgClassName(): String = {
    var parentClass = parentFullClass()
    if (null == parentClass) {
      throw new CGAssertRuntimeException("Union should have a parent class")
    } else {
      "%s.%s".format(parentClass, clazz)
    }
  }
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
    
  val CG_OI_TRANSFORM_MAP = "shark/execution/cg/row/oi/cg_oi_transform_map.ssp"  
  val CG_OI_TRANSFORM_LIST= "shark/execution/cg/row/oi/cg_oi_transform_list.ssp"
  val CG_OI_STRUCT = "shark/execution/cg/row/oi/cg_oi_struct.ssp"  
  val CG_OI_UNION = "shark/execution/cg/row/oi/cg_oi_union.ssp"
  val CG_SF_LIST = "shark/execution/cg/row/oi/cg_sf_list.ssp"
  val CG_SF_MAP = "shark/execution/cg/row/oi/cg_sf_map.ssp"
  val CG_SF_PRIMITIVE = "shark/execution/cg/row/oi/cg_sf_primitive.ssp"
  val CG_SF_STRUCT = "shark/execution/cg/row/oi/cg_sf_struct.ssp"
  val CG_SF_UNION = "shark/execution/cg/row/oi/cg_sf_union.ssp"
  
  val engine = new TemplateEngine
  engine.allowReload  = false
  engine.allowCaching = true

  def layout(ssp: String, map: Map[String, Any]) = {
    engine.layout(ssp, map)
  }
  
  def generate(struct: CGStruct, isOutter: Boolean = false, packageName: String = null): String = {
    CGRow.layout(CGRow.CG_ROW_CLASS_STRUCT, Map("isOutter"->isOutter, "struct" -> struct))
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
    var struct = create(oi, "AA").asInstanceOf[CGStruct]
    struct.packageName = "shark.execution.cg.row"
    
    generate(struct, true, "shark.execution.cg.row")
  }
  
  def generateOI(oi: StructObjectInspector): String = {
    var struct = create(oi, "AA").asInstanceOf[CGStruct]
    struct.packageName = "shark.execution.cg.row"
    
    generateOI(struct, true, "shark.execution.cg.row")
  }
  def generateOI(struct: CGStruct, isOutter: Boolean = false, packageName: String = null): String = {
    CGRow.layout(CGRow.CG_OI_STRUCT, Map("isOutter" -> isOutter, "struct"->struct))
  }
    
  def create(oi: ObjectInspector, name: String): CGField[_<:ObjectInspector] = {
    import collection.JavaConversions._
    oi match {
      case a: StructObjectInspector=> 
        new CGStruct(a,
            name,
            a.getAllStructFieldRefs().map(f => {create(f.getFieldObjectInspector(), f.getFieldName())}).toArray
            )
      case a: ListObjectInspector=>
        new CGList(a,
            name,
            create(a.getListElementObjectInspector(), "%s_l".format(name)) // default name
            )
      case a: MapObjectInspector=>
        new CGMap(a, 
            name,
              create(a.getMapKeyObjectInspector(), "%s_k".format(name)), // default name
              create(a.getMapValueObjectInspector(), "%s_v".format(name)) // default name
              )
      case a: PrimitiveObjectInspector=> new CGPrimitive(a, name)
      case a: UnionObjectInspector=>
        new CGUnion(a,name,
            a.getObjectInspectors().zipWithIndex.map(f=>(f._2, create(f._1, CGRowUtil.unionName(name, f._2)))).toArray 
            )
    }
  }
}
