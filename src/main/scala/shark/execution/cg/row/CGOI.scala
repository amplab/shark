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
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory

import shark.execution.cg.CGAssertRuntimeException

import org.apache.commons.lang.StringUtils

/**
 * Code Generation for Object Inspector, which is depended on the CodeGen Row Object, and providing 
 * the backward-compatiblity for the non-code generated Operators/UDF/Evaluators etc.. 
 * Only Struct & Union has the concrete class, Primitive/List/Map are the field of Struct or Union.
 * Example Of Struct
 * 
 * public class SStruct3StructObjectInspector  extends StructObjectInspector {
  ......
  
  // conform to the StructObjectInspector
  public static class SStruct2StructObjectInspector extends StructObjectInspector {
    // customed StructField
    // list of (field name, StructField class name, StructField class definition)
    // Every field has a StructField definition, which has its type, field, name etc.
    public static final class CGStructField__field___boolean
        extends CGStructField<shark.execution.cg.row.SStruct3.SStruct2, 
        org.apache.hadoop.io.BooleanWritable> {
      public CGStructField__field___boolean(int idx) {
        super(
            "__field___boolean",
            TypeInfoUtils
                .getStandardWritableObjectInspectorFromTypeInfo(TypeInfoFactory.booleanTypeInfo),
            null, idx);
        // The cached Writable Object for primitive type, to avoid creating WritableObject for 
        // every record
        cache = new org.apache.hadoop.io.BooleanWritable();
      }
  
      @Override
      public org.apache.hadoop.io.BooleanWritable get(
          shark.execution.cg.row.SStruct3.SStruct2 data) {
        if (data == null)
          return null;
        if (data.mask.get(idx)) {
          // set the primitive value, if the cg row has the valid field, 
          cache.set(data.__field___boolean);
          return cache;
        } else {
          return null;
        }
      }
  
    }
  
    public static final class CGStructField__field___byte
        extends CGStructField<shark.execution.cg.row.SStruct3.SStruct2, 
             org.apache.hadoop.hive.serde2.io.ByteWritable> {
      public CGStructField__field___byte(int idx) {
        super(
            "__field___byte",
            TypeInfoUtils
                .getStandardWritableObjectInspectorFromTypeInfo(TypeInfoFactory.byteTypeInfo),
            null, idx);
        cache = new org.apache.hadoop.hive.serde2.io.ByteWritable();
      }
  
      @Override
      public org.apache.hadoop.hive.serde2.io.ByteWritable get(
          shark.execution.cg.row.SStruct3.SStruct2 data) {
        if (data == null)
          return null;
        if (data.mask.get(idx)) {
          cache.set(data.__field___byte);
          return cache;
        } else {
          return null;
        }
      }
  
    }
  
    private static final HashMap<String, CGStructField> SF_MAP = 
        new HashMap<String, CGStructField>(55);
    private static final ArrayList<CGStructField> SF_FIELDS = INITIAL_SF();
  
    // initialize the StructFields for all of the fields
    private static ArrayList<CGStructField> INITIAL_SF() {
      ArrayList<CGStructField> fields = new ArrayList<CGStructField>(11);
  
      {
        CGStructField sf = new CGStructField__field___boolean(0);
        SF_MAP.put("boolean".toLowerCase(), sf);
        fields.add(sf);
      }
      {
        CGStructField sf = new CGStructField__field___byte(1);
        SF_MAP.put("byte".toLowerCase(), sf);
        fields.add(sf);
      }
      return fields;
    }
  
    protected ArrayList<Object> writables = initial_writables();
  
    public ArrayList<Object> initial_writables() {
      ArrayList<Object> list = new ArrayList<Object>(11);
      list.add(null);
      list.add(null);
      return list;
    }
  
    // the following is about the StructObjectInspector implementation
    @Override
    public List<? extends CGStructField> getAllStructFieldRefs() {
      return SF_FIELDS;
    }
  
    @Override
    public StructField getStructFieldRef(String fieldName) {
      return SF_MAP.get(fieldName.toLowerCase());
    }
  
    @Override
    public Object getStructFieldData(Object data, StructField fieldRef) {
      if (data == null) {
        return null;
      }
      return ((CGStructField) fieldRef).get(data);
    }
  
    @Override
    public List<Object> getStructFieldsDataAsList(Object data) {
      if (data == null) {
        return null;
      }
  
      shark.execution.cg.row.SStruct3.SStruct2 struct = 
          (shark.execution.cg.row.SStruct3.SStruct2) data;
  
      for (int i = 0; i < SF_FIELDS.size(); ++i) {
        writables.set(i, SF_FIELDS.get(i).get(struct));
      }
  
      return writables;
    }
  
    @Override
    public String getTypeName() {
      return ObjectInspectorUtils.getStandardStructTypeName(this);
    }
  
    @Override
    public Category getCategory() {
      return Category.STRUCT;
    }
  }
}


Example Of Union
public class SStruct3StructObjectInspector  extends StructObjectInspector {
  ......
  // conform to the UnionObjectInspector
  public static class UUnion1UnionObjectInspector implements UnionObjectInspector {
    // objectinspector of all field
    private static final List<ObjectInspector> list = Lists
        .newArrayList(new ObjectInspector[] {
            TypeInfoUtils
                .getStandardWritableObjectInspectorFromTypeInfo(TypeInfoFactory.booleanTypeInfo),
            TypeInfoUtils
                .getStandardWritableObjectInspectorFromTypeInfo(TypeInfoFactory.byteTypeInfo),
  
    // cached writable object for all of the field
    private org.apache.hadoop.io.BooleanWritable cache___field___map_v0 = 
        new org.apache.hadoop.io.BooleanWritable();
    private org.apache.hadoop.hive.serde2.io.ByteWritable cache___field___map_v1 = 
        new org.apache.hadoop.hive.serde2.io.ByteWritable();
  
    @Override
    public String getTypeName() {
      return ObjectInspectorUtils.getStandardUnionTypeName(this);
    }
  
    @Override
    public Category getCategory() {
      return Category.UNION;
    }
  
    @Override
    public List<ObjectInspector> getObjectInspectors() {
      return list;
    }
  
    @Override
    public byte getTag(Object o) {
      shark.execution.cg.row.SStruct3.UUnion1 a = (shark.execution.cg.row.SStruct3.UUnion1) o;
      if (a == null || a.tag < 0) {
        return 0;
      } else {
        return (byte) (a.tag);
      }
    }
  
    @Override
    public Object getField(Object o) {
      shark.execution.cg.row.SStruct3.UUnion1 a = (shark.execution.cg.row.SStruct3.UUnion1) o;
      if (a == null || a.tag < 0) {
        return null;
      } else {
        switch (a.tag) {
        case 0: {
          cache___field___map_v0.set(a.__field___map_v_0);
          return cache___field___map_v0;
        }
        case 1: {
          cache___field___map_v1.set(a.__field___map_v_1);
          return cache___field___map_v1;
        }
        default:
          return null;
        }
      }
    }
  }
}

 * The real case may be more complicated than the primitive-based fields, and more 
 * inner classes generated recursively.
 */
abstract class CGOIField[+T <: CGField[_]](val delegate: T, protected val order: Int) {
  lazy val dataContainerClassName: String = delegate.parentFullClassName

  lazy val dataClassName: String = delegate.fullClassName

  lazy val dataJavaClass: String = CGOIField.dataJavaClass(this)
  lazy val dataWritableClass: String = CGOIField.dataWritableClass(this)
  lazy val defCreateOI: String = CGOIField.defCreateOI(this)
  lazy val defStructField: String = CGOIField.defStructField(this)
  lazy val defStructFieldTransform: String = CGOIField.defStructFieldTransform(this)
  lazy val defStaticBlocks: String = CGOIField.defStaticBlocks(this)
  lazy val structFieldClassName = CGOIField.getSFClassName(delegate)

  def java2Writable(javaVarName: String, writableVarName: String): String = 
    CGOIField.java2Writable(this, javaVarName, writableVarName)
}

abstract class CGOIPrimitive[+T <: CGField[_]](delegate: T, order: Int) 
  extends CGOIField(delegate, order)

class CGOIPrimitiveBinary(delegate: CGPrimitiveBinary, order: Int) 
  extends CGOIPrimitive(delegate, order)

class CGOIPrimitiveBoolean(delegate: CGPrimitiveBoolean, order: Int) 
  extends CGOIPrimitive(delegate, order)

class CGOIPrimitiveByte(delegate: CGPrimitiveByte, order: Int) 
  extends CGOIPrimitive(delegate, order)

class CGOIPrimitiveDouble(delegate: CGPrimitiveDouble, order: Int) 
  extends CGOIPrimitive(delegate, order)

class CGOIPrimitiveFloat(delegate: CGPrimitiveFloat, order: Int) 
  extends CGOIPrimitive(delegate, order)

class CGOIPrimitiveInt(delegate: CGPrimitiveInt, order: Int) 
  extends CGOIPrimitive(delegate, order)

class CGOIPrimitiveLong(delegate: CGPrimitiveLong, order: Int) 
  extends CGOIPrimitive(delegate, order)

class CGOIPrimitiveShort(delegate: CGPrimitiveShort, order: Int) 
  extends CGOIPrimitive(delegate, order)

class CGOIPrimitiveString(delegate: CGPrimitiveString, order: Int) 
  extends CGOIPrimitive(delegate, order)

class CGOIPrimitiveTimestamp(delegate: CGPrimitiveTimestamp, order: Int) 
  extends CGOIPrimitive(delegate, order)

class CGOIMap(delegate: CGMap, 
    val key: CGOIField[_ <: CGField[_]], 
    val value: CGOIField[_ <: CGField[_]], 
    order: Int) 
  extends CGOIField[CGMap](delegate, order)
  
class CGOIList(delegate: CGList, 
    val field: CGOIField[_ <: CGField[_]], 
    order: Int) 
  extends CGOIField[CGList](delegate, order)

class CGOIStruct(delegate: CGStruct, val fields: Array[CGOIField[_ <: CGField[_]]], order: Int) 
  extends CGOIField[CGStruct](delegate, order) {
  lazy val clazz: String = CGOIField.getOIClassName(delegate)

  lazy val fullClassName: String = CGOIField.fullClassName(this)
}

class CGOIUnion(delegate: CGUnion, val fields: Array[CGOIField[_ <: CGField[_]]], order: Int) 
  extends CGOIField[CGUnion](delegate, order) {
  lazy val clazz: String = CGOIField.getOIClassName(delegate)

  lazy val fullClassName: String = CGOIField.fullClassName(this)
}

object CGOIField {
  def create(field: CGField[_]): CGOIField[_ <: CGField[_]] = {
    import collection.JavaConversions._
    field match {
      case a: CGStruct =>
        new CGOIStruct(a,
          Array.tabulate[CGOIField[_ <: CGField[_]]](a.fields.length) { i =>
            create(a.fields(i))
          }, a.order)
      case a: CGList => new CGOIList(a, create(a.field), a.order)
      case a: CGMap  => new CGOIMap(a, create(a.key), create(a.value), a.order)
      case a: CGUnion =>
        new CGOIUnion(a,
          Array.tabulate[CGOIField[_ <: CGField[_]]](a.fields.length) { i =>
            create(a.fields(i))
          }, a.order)
      case a: CGPrimitiveBinary    => new CGOIPrimitiveBinary(a, a.order)
      case a: CGPrimitiveBoolean   => new CGOIPrimitiveBoolean(a, a.order)
      case a: CGPrimitiveByte      => new CGOIPrimitiveByte(a, a.order)
      case a: CGPrimitiveDouble    => new CGOIPrimitiveDouble(a, a.order)
      case a: CGPrimitiveFloat     => new CGOIPrimitiveFloat(a, a.order)
      case a: CGPrimitiveInt       => new CGOIPrimitiveInt(a, a.order)
      case a: CGPrimitiveLong      => new CGOIPrimitiveLong(a, a.order)
      case a: CGPrimitiveShort     => new CGOIPrimitiveShort(a, a.order)
      case a: CGPrimitiveString    => new CGOIPrimitiveString(a, a.order)
      case a: CGPrimitiveTimestamp => new CGOIPrimitiveTimestamp(a, a.order)
    }
  }

  def getOIClassName(field: CGField[_]): String = {
    field match {
      case f: CGStruct => "%sStructObjectInspector".format(f.clazz)
      case f: CGUnion  => "%sUnionObjectInspector".format(f.clazz)
      case f: Any => throw new CGAssertRuntimeException("Couldn't find the OI class name for " + f)
    }
  }

  def getSFClassName(field: CGField[_]): String = "CGStructField%s".format(field.name)

  private[this] val DEF_CREATE_OI = Array[(CGOIField[_]) => String](
    (f) => "TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(TypeInfoFactory.binaryTypeInfo)",
    (f) => "TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(TypeInfoFactory.booleanTypeInfo)",
    (f) => "TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(TypeInfoFactory.byteTypeInfo)",
    (f) => "TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(TypeInfoFactory.dateTypeInfo)",
    (f) => "TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(TypeInfoFactory.doubleTypeInfo)",
    (f) => "TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(TypeInfoFactory.floatTypeInfo)",
    (f) => "TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(TypeInfoFactory.intTypeInfo)",
    (f) => "TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(TypeInfoFactory.longTypeInfo)",
    (f) => "TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(TypeInfoFactory.shortTypeInfo)",
    (f) => "TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(TypeInfoFactory.stringTypeInfo)",
    (f) => "TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(TypeInfoFactory.timestampTypeInfo)",
    (f) => {
      val field = f.asInstanceOf[CGOIMap]
      "ObjectInspectorFactory.getStandardMapObjectInspector(%s,%s)".format(
        field.key.defCreateOI,
        field.value.defCreateOI)
    },
    (f) => {
      val field = f.asInstanceOf[CGOIList]
      "ObjectInspectorFactory.getStandardListObjectInspector(%s)".format(field.field.defCreateOI)
    },
    (f) => {
      val field = f.asInstanceOf[CGOIStruct]
      "new %s()".format(field.clazz)
    },
    (f) => {
      val field = f.asInstanceOf[CGOIUnion]
      "new %s()".format(field.clazz)
    }
  )

  private[this] val DATA_WRITABLE_CLASS = Array[(CGOIField[_]) => String](
    (f) => "org.apache.hadoop.io.BytesWritable",
    (f) => "org.apache.hadoop.io.BooleanWritable",
    (f) => "org.apache.hadoop.hive.serde2.io.ByteWritable",
    (f) => "org.apache.hadoop.hive.serde2.io.DateWritable",
    (f) => "org.apache.hadoop.hive.serde2.io.DoubleWritable",
    (f) => "org.apache.hadoop.io.FloatWritable",
    (f) => "org.apache.hadoop.io.IntWritable",
    (f) => "org.apache.hadoop.io.LongWritable",
    (f) => "org.apache.hadoop.hive.serde2.io.ShortWritable",
    (f) => "org.apache.hadoop.io.Text",
    (f) => "org.apache.hadoop.hive.serde2.io.TimestampWritable",
    (f) => {
      val field = f.asInstanceOf[CGOIMap]
      "java.util.HashMap<%s, %s>".format(field.key.dataWritableClass, field.value.dataWritableClass)
    },
    (f) => {
      val field = f.asInstanceOf[CGOIList]
      "java.util.ArrayList<%s>".format(field.field.dataWritableClass)
    },
    (f) => {
      val field = f.asInstanceOf[CGOIStruct]
      field.dataClassName
    },
    (f) => {
      val field = f.asInstanceOf[CGOIUnion]
      field.dataClassName
    }
  )

  private[this] val JAVA_2_WRITABLE_EXPRESSION = Array[(CGOIField[_], String) => String](
    (f, data) => "new org.apache.hadoop.io.BytesWritable(%s)".format(data),
    (f, data) => "new org.apache.hadoop.io.BooleanWritable(%s)".format(data),
    (f, data) => "new org.apache.hadoop.hive.serde2.io.ByteWritable(%s)".format(data),
    (f, data) => "new org.apache.hadoop.hive.serde2.io.DateWritable(%s)".format(data),
    (f, data) => "new org.apache.hadoop.hive.serde2.io.DoubleWritable(%s)".format(data),
    (f, data) => "new org.apache.hadoop.io.FloatWritable(%s)".format(data),
    (f, data) => "new org.apache.hadoop.io.IntWritable(%s)".format(data),
    (f, data) => "new org.apache.hadoop.io.LongWritable(%s)".format(data),
    (f, data) => "new org.apache.hadoop.hive.serde2.io.ShortWritable(%s)".format(data),
    (f, data) => "new org.apache.hadoop.io.Text(%s)".format(data),
    (f, data) => "new org.apache.hadoop.hive.serde2.io.TimestampWritable(%s)".format(data),
    (f, data) => {
      "%s_transform(%s)".format(f.asInstanceOf[CGOIMap].delegate.name, data)
    },
    (f, data) => {
      "%s_transform(%s)".format(f.asInstanceOf[CGOIList].delegate.name, data)
    },
    (f, data) => {
      data
    },
    (f, data) => {
      data
    }
  )

  private[this] val JAVA_2_WRITABLE_SENTENCE = Array[(CGOIField[_], String, String) => String](
    (f, data, obj) => "%s.set(%s, 0, %s.length);".format(obj, data, data),
    (f, data, obj) => "%s.set(%s);".format(obj, data),
    (f, data, obj) => "%s.set(%s);".format(obj, data),
    (f, data, obj) => "%s.set(%s);".format(obj, data),
    (f, data, obj) => "%s.set(%s);".format(obj, data),
    (f, data, obj) => "%s.set(%s);".format(obj, data),
    (f, data, obj) => "%s.set(%s);".format(obj, data),
    (f, data, obj) => "%s.set(%s);".format(obj, data),
    (f, data, obj) => "%s.set(%s);".format(obj, data),
    (f, data, obj) => "%s.set(%s);".format(obj, data),
    (f, data, obj) => "%s.set(%s);".format(obj, data),
    (f, data, obj) => {
      "%s=%s_transform(%s);".format(obj, f.asInstanceOf[CGOIMap].delegate.name, data)
    },
    (f, data, obj) => {
      "%s = %s_transform(%s);".format(obj, f.asInstanceOf[CGOIList].delegate.name, data)
    },
    (f, data, obj) => {
      "%s=%s;".format(obj, data)
    },
    (f, data, obj) => {
      "%s=%s;".format(obj, data)
    }
  )

  private[this] val DEF_STRUCT_FIELD = Array[((AnyRef)) => String](
    (f) => CGTE.layout(CGOI.CG_SF_PRIMITIVE, Map("obj" -> f)),
    (f) => CGTE.layout(CGOI.CG_SF_PRIMITIVE, Map("obj" -> f)),
    (f) => CGTE.layout(CGOI.CG_SF_PRIMITIVE, Map("obj" -> f)),
    (f) => CGTE.layout(CGOI.CG_SF_PRIMITIVE, Map("obj" -> f)),
    (f) => CGTE.layout(CGOI.CG_SF_PRIMITIVE, Map("obj" -> f)),
    (f) => CGTE.layout(CGOI.CG_SF_PRIMITIVE, Map("obj" -> f)),
    (f) => CGTE.layout(CGOI.CG_SF_PRIMITIVE, Map("obj" -> f)),
    (f) => CGTE.layout(CGOI.CG_SF_PRIMITIVE, Map("obj" -> f)),
    (f) => CGTE.layout(CGOI.CG_SF_PRIMITIVE, Map("obj" -> f)),
    (f) => CGTE.layout(CGOI.CG_SF_PRIMITIVE, Map("obj" -> f)),
    (f) => CGTE.layout(CGOI.CG_SF_PRIMITIVE, Map("obj" -> f)),
    (f) => CGTE.layout(CGOI.CG_SF_MAP, Map("obj" -> f)),
    (f) => CGTE.layout(CGOI.CG_SF_LIST, Map("obj" -> f)),
    (f) => CGTE.layout(CGOI.CG_SF_STRUCT, Map("obj" -> f)),
    (f) => CGTE.layout(CGOI.CG_SF_UNION, Map("obj" -> f))
  )

  private[this] val DEF_STRUCT_FIELD_TRANSFORM = Array[((AnyRef)) => String](
    (f) => null,
    (f) => null,
    (f) => null,
    (f) => null,
    (f) => null,
    (f) => null,
    (f) => null,
    (f) => null,
    (f) => null,
    (f) => null,
    (f) => null,
    (f) => CGTE.layout(CGOI.CG_OI_TRANSFORM_MAP, Map("obj" -> f)),
    (f) => CGTE.layout(CGOI.CG_OI_TRANSFORM_LIST, Map("obj" -> f)),
    (f) => null,
    (f) => null
  )

  private[this] val DEF_STATIC_BLOCK = Array[((AnyRef)) => String](
    (f) => null,
    (f) => null,
    (f) => null,
    (f) => null,
    (f) => null,
    (f) => null,
    (f) => null,
    (f) => null,
    (f) => null,
    (f) => null,
    (f) => null,
    (f) => {
      val field = f.asInstanceOf[CGOIMap]
      StringUtils.join(Array(field.key.defStaticBlocks, "\n", field.value.defStaticBlocks))
    },
    (f) => f.asInstanceOf[CGOIList].field.defStaticBlocks,
    (f) => CGTE.layout(CGOI.CG_OI_STRUCT, Map("struct" -> f)),
    (f) => CGTE.layout(CGOI.CG_OI_UNION, Map("obj" -> f))
  )

  private[this] val DATA_JAVACLASS = Array[(CGOIField[_]) => String](
    (f) => f.asInstanceOf[CGOIPrimitiveBinary].delegate.clazz,
    (f) => f.asInstanceOf[CGOIPrimitiveBoolean].delegate.clazz,
    (f) => f.asInstanceOf[CGOIPrimitiveByte].delegate.clazz,
    (f) => f.asInstanceOf[CGOIPrimitiveDouble].delegate.clazz,
    (f) => f.asInstanceOf[CGOIPrimitiveFloat].delegate.clazz,
    (f) => f.asInstanceOf[CGOIPrimitiveInt].delegate.clazz,
    (f) => f.asInstanceOf[CGOIPrimitiveLong].delegate.clazz,
    (f) => f.asInstanceOf[CGOIPrimitiveShort].delegate.clazz,
    (f) => f.asInstanceOf[CGOIPrimitiveString].delegate.clazz,
    (f) => f.asInstanceOf[CGOIPrimitiveTimestamp].delegate.clazz,
    (f) => {
      val field = f.asInstanceOf[CGOIMap]
      "java.util.HashMap<%s, %s>".format(field.key.dataJavaClass, field.value.dataJavaClass)
    },
    (f) => "java.util.ArrayList<%s>".format(f.asInstanceOf[CGOIList].field.dataJavaClass),
    (f) => f.asInstanceOf[CGOIStruct].dataWritableClass,
    (f) => f.asInstanceOf[CGOIUnion].dataWritableClass
  )

  def java2Writable(f: CGOIField[_], data: String, obj: String) = {
    if (obj == null) {
      JAVA_2_WRITABLE_EXPRESSION(f.order)(f, data)
    } else {
      JAVA_2_WRITABLE_SENTENCE(f.order)(f, data, obj)
    }
  }

  def defStructField(f: CGOIField[_]) = DEF_STRUCT_FIELD(f.order)(f)
  def defStructFieldTransform(f: CGOIField[_]) = DEF_STRUCT_FIELD_TRANSFORM(f.order)(f)
  def defStaticBlocks(f: CGOIField[_]) = DEF_STATIC_BLOCK(f.order)(f)
  def defCreateOI(f: CGOIField[_]) = DEF_CREATE_OI(f.order)(f)
  def dataWritableClass(f: CGOIField[_]) = DATA_WRITABLE_CLASS(f.order)(f)
  def dataJavaClass(f: CGOIField[_]) = DATA_JAVACLASS(f.order)(f)
  def fullClassName(struct: CGOIStruct): String = {
    if (null == struct.dataContainerClassName) {
      "%s.%s".format(CGField.PACKAGE_NAME, struct.clazz)
    } else {
      "%s$%s".format(struct.dataContainerClassName, struct.clazz)
    }
  }
  
  def fullClassName(union: CGOIUnion): String = 
    if (null == union.dataContainerClassName) {
      throw new CGAssertRuntimeException("Union should have a parent class")
    } else {
      "%s$%s".format(union.dataContainerClassName, union.clazz)
    }
}

object CGOI {
  // TODO need to precompile those templates
  val CG_OI_TRANSFORM_MAP = "shark/execution/cg/row/oi/cg_oi_transform_map.ssp"
  val CG_OI_TRANSFORM_LIST = "shark/execution/cg/row/oi/cg_oi_transform_list.ssp"
  val CG_OI_STRUCT = "shark/execution/cg/row/oi/cg_oi_struct.ssp"
  val CG_OI_UNION = "shark/execution/cg/row/oi/cg_oi_union.ssp"
  val CG_SF_LIST = "shark/execution/cg/row/oi/cg_sf_list.ssp"
  val CG_SF_MAP = "shark/execution/cg/row/oi/cg_sf_map.ssp"
  val CG_SF_PRIMITIVE = "shark/execution/cg/row/oi/cg_sf_primitive.ssp"
  val CG_SF_STRUCT = "shark/execution/cg/row/oi/cg_sf_struct.ssp"
  val CG_SF_UNION = "shark/execution/cg/row/oi/cg_sf_union.ssp"

  def generateOI(struct: CGOIStruct, isOutter: Boolean = false): String = {
    CGTE.layout(CGOI.CG_OI_STRUCT, Map("isOutter" -> isOutter, "struct" -> struct))
  }
}
