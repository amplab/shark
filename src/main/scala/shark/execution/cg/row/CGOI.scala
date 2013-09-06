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

import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector
import shark.execution.cg.CGAssertRuntimeException
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory

abstract class CGOIField[+T<:CGField[_]](val delegate: T) {
  def parentFullClassName(): String = delegate.parentFullClassName()
  def fullClassName(): String = delegate.fullClassName()
  
  def createOI(): String
  def writableClass(): String
  def java2Writable(javaVarName: String, writableVarName: String): (String, ()=>String) // (code snippet for the conversion, code block (functions) for extra)
  def defStructField(): (String, String, String) // (field name, class name, struct field class definition)
  def oiClass(): String
}

class CGOIPrimitive(delegate: CGPrimitive) extends CGOIField[CGPrimitive](delegate) {
  override def createOI(): String = delegate.oi.getPrimitiveCategory() match {
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
  override def writableClass(): String = delegate.oi.getPrimitiveCategory() match {
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
      delegate.oi.getPrimitiveCategory() match {
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
      delegate.oi.getPrimitiveCategory() match {
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
    var classname = "CGStructField%s".format(delegate.name)
    (delegate.name, classname, CGTE.layout(CGOI.CG_SF_PRIMITIVE, Map("obj"->this, "classname"->classname)))
  }
  
  override def oiClass(): String = null
}

class CGOIMap(delegate: CGMap, val key: CGOIField[_<:CGField[_]], val value: CGOIField[_<:CGField[_]]) extends CGOIField[CGMap](delegate) {
  override def createOI(): String = "ObjectInspectorFactory.getStandardMapObjectInspector(%s,%s)".format(key.createOI(), value.createOI())
  override def writableClass(): String = "java.util.HashMap<%s, %s>".format(key.writableClass(), value.writableClass())
    
  override def java2Writable(javaVarName: String, writableVarName: String) = {
    var methodName = "%s_%s_transform".format(delegate.name, javaVarName)
    (if (writableVarName!= null) 
      "%s=%s(%s);".format(writableVarName, methodName, javaVarName)
     else
      "%s(%s)".format(methodName, javaVarName), ()=>{
      CGTE.layout(CGOI.CG_OI_TRANSFORM_MAP, 
        Map("obj" -> this, "methodName" -> methodName))
    })
  }
  
  override def defStructField(): (String, String, String) = {
    var classname = "CGStructField%s".format(delegate.name)
    (delegate.name, classname, CGTE.layout(CGOI.CG_SF_MAP, Map("obj"->this, "classname"->classname)))
  }
  
  override def oiClass(): String = null
}

class CGOIList(delegate: CGList, val field: CGOIField[_<:CGField[_]]) extends CGOIField[CGList](delegate) {
    // the following methods are used for ObjectInspector generation
  override def createOI(): String = "ObjectInspectorFactory.getStandardListObjectInspector(%s)".format(field.createOI())
  
  override def writableClass(): String = "java.util.ArrayList<%s>".format(field.writableClass())
      
  override def java2Writable(javaVarName: String, writableVarName: String) = {
    var methodName = if(javaVarName == null) 
                        "%s_transform".format(delegate.name)
                     else
                        "%s_%s_transform".format(delegate.name, javaVarName)
    (if (writableVarName!=null)
      "%s = %s(%s);".format(writableVarName, methodName, javaVarName)
     else
      "%s(%s)".format(methodName, javaVarName), ()=>{
      CGTE.layout(CGOI.CG_OI_TRANSFORM_LIST, 
        Map("obj" -> this, "methodName" -> methodName))
    })
  }
  
  override def defStructField(): (String, String, String) = {
    var classname = "CGStructField%s".format(delegate.name)
    (delegate.name, classname, CGTE.layout(CGOI.CG_SF_LIST, Map("obj"->this, "classname"->classname)))
  }
  
  override def oiClass(): String = null
}

class CGOIStruct(delegate: CGStruct, val fields:Array[CGOIField[_<:CGField[_]]]) extends CGOIField[CGStruct](delegate) {
  def oiClassName(): String = "%sStructObjectInspector".format(delegate.clazz)
  override def createOI(): String = "new %s()".format(oiClassName())
  override def writableClass(): String = delegate.fullClassName()
  override def java2Writable(javaVarName: String, writableVarName: String) = 
    (if(writableVarName!=null)
      "%s=%s;".format(writableVarName, javaVarName)
     else
       javaVarName, ()=>null)
  override def defStructField(): (String, String, String) = {
    var classname = "CGStructField%s".format(delegate.name)
    (delegate.name, classname, CGTE.layout(CGOI.CG_SF_STRUCT, Map("obj"->this, "classname"->classname)))
  }
  
  override def oiClass(): String = {CGTE.layout(CGOI.CG_OI_STRUCT, Map("struct"->this))}
  
  override def fullClassName(): String = {
    var parentClass = parentFullClassName()
    if (null == parentClass) {
      if (delegate.packageName != null) "%s.%s".format(delegate.packageName, oiClassName) else oiClassName
    } else {
      "%s.%s".format(parentClass, oiClassName)
    }
  }
}

class CGOIUnion(delegate: CGUnion, val fields: Array[CGOIField[_<:CGField[_]]]) extends CGOIField[CGUnion](delegate) {
  def oiClassName(): String = "%sUnionObjectInspector".format(delegate.clazz)
  override def createOI(): String = "new %s()".format(oiClassName())
  override def writableClass(): String = fullClassName()
  override def java2Writable(javaVarName: String, writableVarName: String) = 
    (if(writableVarName!=null)
      "%s=%s;".format(writableVarName, javaVarName)
     else
       javaVarName, ()=>null)
  override def defStructField(): (String, String, String) = {
    var classname = "CGStructField%s".format(delegate.name)
    (delegate.name, classname, CGTE.layout(CGOI.CG_SF_UNION, Map("obj"->this, "classname"->classname)))
  }  
  override def oiClass(): String = {CGTE.layout(CGOI.CG_OI_UNION, Map("obj"->this))}
}

object CGOIField {
  def create(field: CGField[_]): CGOIField[_ <: CGField[_]] = {
    import collection.JavaConversions._
    field match {
      case a: CGStruct =>
        new CGOIStruct(a,
          Array.tabulate[CGOIField[_<:CGField[_]]](a.fields.length) { i =>
            create(a.fields(i))
          }
        )
      case a: CGList => new CGOIList(a, create(a.field))
      case a: CGMap => new CGOIMap(a, create(a.key), create(a.value))
      case a: CGPrimitive => new CGOIPrimitive(a)
      case a: CGUnion =>
        new CGOIUnion(a,
          Array.tabulate[CGOIField[_<:CGField[_]]](a.fields.length) { i =>
            create(a.fields(i))
          }
        )
    }
  }
}

object CGOI {
  val CG_OI_TRANSFORM_MAP = "shark/execution/cg/row/oi/cg_oi_transform_map.ssp"  
  val CG_OI_TRANSFORM_LIST= "shark/execution/cg/row/oi/cg_oi_transform_list.ssp"
  val CG_OI_STRUCT = "shark/execution/cg/row/oi/cg_oi_struct.ssp"  
  val CG_OI_UNION = "shark/execution/cg/row/oi/cg_oi_union.ssp"
  val CG_SF_LIST = "shark/execution/cg/row/oi/cg_sf_list.ssp"
  val CG_SF_MAP = "shark/execution/cg/row/oi/cg_sf_map.ssp"
  val CG_SF_PRIMITIVE = "shark/execution/cg/row/oi/cg_sf_primitive.ssp"
  val CG_SF_STRUCT = "shark/execution/cg/row/oi/cg_sf_struct.ssp"
  val CG_SF_UNION = "shark/execution/cg/row/oi/cg_sf_union.ssp"

  def generateOI(struct: CGOIStruct, isOutter: Boolean = false): String = {
    CGTE.layout(CGOI.CG_OI_STRUCT, Map("isOutter" -> isOutter, "struct"->struct))
  }
}
