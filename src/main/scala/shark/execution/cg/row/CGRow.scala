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

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.ObjectOutputStream
import java.io.ObjectInputStream

import scala.reflect.BeanProperty
import scala.collection.mutable.ArrayBuffer

import org.fusesource.scalate.TemplateEngine
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
import org.apache.hadoop.hive.serde2.io.ByteWritable

import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo
import org.apache.hadoop.hive.serde2.typeinfo.{TypeInfoFactory => TIF}
import org.apache.hadoop.hive.serde2.objectinspector.StandardConstantMapObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.StandardConstantListObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.UnionObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantBinaryObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantBooleanObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantByteObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantTimestampObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantStringObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantShortObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantLongObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantIntObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantFloatObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantDoubleObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableBinaryObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.VoidObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ByteObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ShortObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.StructField

import shark.execution.cg.CGAssertRuntimeException
import shark.execution.cg.CGUtil


/**
 * Code Generation for CGRow, which utilizes the Scala Template Engine (ScalaTE), to generate 
 * the java source code.
 * Only Struct & Union has the concrete class, Primitive/List/Map are the field of Struct or Union.
 * 
 * Example Of Struct
 * 
public class Struct3 implements KryoSerializable {
  ...
  // constant field initializing
  public static final transient HashMap<Integer, SStruct2> 
      __field___hashmap = INITIAL___field___hashmap();
  ...



  public static class SStruct2 implements KryoSerializable {
  
    public static final transient int MASK___FIELD___BOOLEAN = 0;
    public static final transient int MASK___FIELD___BYTE = 1;
  
    public SStruct2() {
      mask.set(shark.execution.cg.row.SStruct3.SStruct2.MASK___FIELD___BOOLEAN, false);
      mask.set(shark.execution.cg.row.SStruct3.SStruct2.MASK___FIELD___BYTE, false);
    }
  
    public void reset() {
      mask.set(shark.execution.cg.row.SStruct3.SStruct2.MASK___FIELD___BOOLEAN, false);
      mask.set(shark.execution.cg.row.SStruct3.SStruct2.MASK___FIELD___BYTE, false);
    }
  
    // the bit mask to store the fields' validity 
    public BitSet mask = new BitSet(2);
  
    public boolean __field___boolean;
    public byte __field___byte;
  
    @Override
    public void read(Kryo kryo, Input input) {
      // TODO need to figure out how to serialize the BitSet more efficiently
      // in Sun jdk 1.7 is quite easy, but not in jdk 1.6
      mask = kryo.readObject(input, BitSet.class);
      if (mask.get(MASK___FIELD___BOOLEAN)) {
        __field___boolean = input.readBoolean();
      }
      if (mask.get(MASK___FIELD___BYTE)) {
        __field___byte = input.readByte();
      }
    }
  
    @Override
    public void write(Kryo kryo, Output output) {
      // TODO need to figure out how to serialize the BitSet more efficiently
      // in Sun jdk 1.7 is quite easy, but not in jdk 1.6
      kryo.writeObject(output, mask);
      if (mask.get(MASK___FIELD___BOOLEAN)) {
        output.writeBoolean(__field___boolean);
      }
      if (mask.get(MASK___FIELD___BYTE)) {
        output.writeByte(__field___byte);
      }
    }
  
    @Override
    public int hashCode() {
      int ____hash_code____ = 0;
      if (mask.get(MASK___FIELD___BOOLEAN)) {
        ____hash_code____ += org.apache.commons.lang.ObjectUtils
            .hashCode(__field___boolean);
      }
      if (mask.get(MASK___FIELD___BYTE)) {
        ____hash_code____ += org.apache.commons.lang.ObjectUtils
            .hashCode(__field___byte);
      }
  
      return ____hash_code____;
    }
  
    @Override
      public boolean equals(Object other) {
          if (null == other) return false;
          if (other instanceof SStruct2) {
              SStruct2 ____that____ = (SStruct2)other;
              if (mask.get(MASK___FIELD___BOOLEAN) != ____that____.mask.get(MASK___FIELD___BOOLEAN)) 
            {
                return false;
            } else if (mask.get(MASK___FIELD___BOOLEAN) && 
                (!org.apache.commons.lang.ObjectUtils.equals(__field___boolean, 
                    ____that____.__field___boolean))) {
                return false;
            }
              if (mask.get(MASK___FIELD___BYTE) != ____that____.mask.get(MASK___FIELD___BYTE)) {
                return false;
            } else if (mask.get(MASK___FIELD___BYTE) && 
                (!org.apache.commons.lang.ObjectUtils.equals(__field___byte, 
                    ____that____.__field___byte))) {
                return false;
            } else {
              return false;
          }
      }
  
    // Create the CGRow object from the ObjectInspector & its Data
    public static SStruct2 BUILD(ObjectInspector _oi, Object o) {
      if (o == null)
        return null;
      SStruct2 me = new SStruct2();
      StructObjectInspector oi = (StructObjectInspector) _oi;
      {
        org.apache.hadoop.hive.serde2.objectinspector.StructField sf = oi
            .getStructFieldRef("boolean");
        Object obj = oi.getStructFieldData(o, sf);
  
        if (obj == null) {
          me.mask.set(
              shark.execution.cg.row.SStruct3.SStruct2.MASK___FIELD___BOOLEAN,
              false);
        } else {
          me.mask.set(
              shark.execution.cg.row.SStruct3.SStruct2.MASK___FIELD___BOOLEAN,
              true);
          me.__field___boolean = 
              ((org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector) 
                  sf.getFieldObjectInspector()).get(obj);
        }
      }
      {
        org.apache.hadoop.hive.serde2.objectinspector.StructField sf = oi
            .getStructFieldRef("byte");
        Object obj = oi.getStructFieldData(o, sf);
  
        if (obj == null) {
          me.mask
              .set(shark.execution.cg.row.SStruct3.SStruct2.MASK___FIELD___BYTE,
                  false);
        } else {
          me.mask.set(
              shark.execution.cg.row.SStruct3.SStruct2.MASK___FIELD___BYTE, true);
          me.__field___byte = 
              ((org.apache.hadoop.hive.serde2.objectinspector.primitive.ByteObjectInspector) 
                  sf.getFieldObjectInspector()).get(obj);
        }
      }
  
      return me;
    }
  
    /**
     * This is used internally, for creating a constant value by passed in value.
     */
    public static SStruct2 INITIAL(Object o) {
      if (o == null) {
        return null;
      }
      // array => v1, v2, v3, ....
      Object[] objs = (Object[]) o;
  
      SStruct2 f = new SStruct2();
  
      {
        if (objs[0] != null) {
          f.mask.set(
              shark.execution.cg.row.SStruct3.SStruct2.MASK___FIELD___BOOLEAN,
              true);
          f.__field___boolean = (Boolean) objs[0];
        }
      }
      {
        if (objs[1] != null) {
          f.mask.set(
              shark.execution.cg.row.SStruct3.SStruct2.MASK___FIELD___BYTE, true);
          f.__field___byte = (Byte) objs[1];
        }
      }
      return f;
    }
  }
  
  private static java.util.HashMap<Integer, SStruct2> INITIAL___field___hashmap() {
    // serialized value of HashMap<Integer, SStruct2>, which was done the via dataConstantOI2Java()
    byte[] content = new byte[]{(byte)0x1,(byte)0x0,(byte)0x5b,(byte)0x4c,(byte)0x6a,(byte)0x61,
       (byte)0x76,(byte)0x61,(byte)0x2e,(byte)0x6c,(byte)0x61,(byte)0x6e,(byte)0x67,(byte)0x2e,
       (byte)0x4f,(byte)0x62,(byte)0x6a,(byte)0x65,(byte)0x63,(byte)0x74,(byte)0xbb,(byte)0x1};
    
    Object o = shark.execution.cg.CGUtil.deserialize(content);
    return INITIAL___field___hashmap(o);
  }

  // initialize the constant field
  private static java.util.HashMap<Integer, SStruct2> INITIAL___field___hashmap(Object o) {
    if (null == o) {
        return null;
    }
    Object[] objs = (Object[])o;
    
    java.util.HashMap<Integer, SStruct2> map = new java.util.HashMap<Integer, SStruct2>();
    for(int i = 0; i < objs.length; i += 2) {
      Integer k = null;
      SStruct2 v = null;
      Object ok = objs[i];
      Object ov = objs[i + 1];
      k = (Integer)ok;
      v = shark.execution.cg.row.SStruct3.SStruct2.INITIAL(ov);
       
      map.put(k, v);
    }
    
    return map;
  }
}
 * 
 * 
 * 
 * Example Of Union
 * public static class UUnion1 implements KryoSerializable {
  // end of sub type definition

  // tag < 0 means the union value is null.
  public byte tag = (byte) -1;

  public void reset() {
    this.tag = (byte) -1;
  }

  public boolean __field___map_v_0;
  public byte __field___map_v_1;

  @Override
  public void read(Kryo kryo, Input input) {
    tag = input.readByte();
    switch (tag) {
    case 0:
      __field___map_v_0 = input.readBoolean();
      break;
    case 1:
      __field___map_v_1 = input.readByte();
      break;
    default:
    }
  }

  @Override
  public void write(Kryo kryo, Output output) {
    output.writeByte(tag);
    switch (tag) {
    case 0:
      output.writeBoolean(__field___map_v_0);
      break;
    case 1:
      output.writeByte(__field___map_v_1);
      break;
    default:
    }
  }

  @Override
  public int hashCode() {
    switch (tag) {
    case 0:
      return tag
          + org.apache.commons.lang.ObjectUtils.hashCode(__field___map_v_0);
    case 1:
      return tag
          + org.apache.commons.lang.ObjectUtils.hashCode(__field___map_v_1);
    }
    return 0;
  }

  @Override
  public boolean equals(Object other) {
    if (null == other)
      return false;
    if (other instanceof UUnion1) {
      UUnion1 ____that____ = (UUnion1) other;
      if (tag != ____that____.tag) {
        return false;
      }
      switch (tag) {
      case 0:

        return org.apache.commons.lang.ObjectUtils.equals(__field___map_v_0,
            ____that____.__field___map_v_0);
      case 1:

        return org.apache.commons.lang.ObjectUtils.equals(__field___map_v_1,
            ____that____.__field___map_v_1);

        return true;
      }
    }
    return false;
  }

  // Create Union Object by ObjectInspector & its Data
  public static UUnion1 BUILD(ObjectInspector oi, Object o) {
    if (o == null)
      return null;
    UUnion1 me = new UUnion1();
    org.apache.hadoop.hive.serde2.objectinspector.UnionObjectInspector uoi = 
        (org.apache.hadoop.hive.serde2.objectinspector.UnionObjectInspector) oi;
    Object uobj = uoi.getField(o);
    if (uobj == null) {
      me.tag = -1;
    } else {
      me.tag = uoi.getTag(o);
      java.util.List<org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector> ois = uoi
          .getObjectInspectors();

      switch (me.tag) {
      case 0:
        me.__field___map_v_0 = 
          ((org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector) ois
            .get(0)).get(uobj);
        ;
        break;
      case 1:
        me.__field___map_v_1 = 
          ((org.apache.hadoop.hive.serde2.objectinspector.primitive.ByteObjectInspector) ois
            .get(1)).get(uobj);
        ;
        break;
      default: {
        throw new RuntimeException("can't find the tag:" + me.tag);
      }
      }
    }
    return me;
  }

  /**
   * create UUnion1 from java object (array)
   */
  public static UUnion1 INITIAL(Object o) {
    if (o == null) {
      return null;
    }

    // array => [tag, object]
    Object[] objs = (Object[]) o;

    UUnion1 c = new UUnion1();

    c.tag = (Byte) objs[0];
    switch (c.tag) {
    case 0:
      c.__field___map_v_0 = (Boolean) objs[1];
      break;
    case 1:
      c.__field___map_v_1 = (Byte) objs[1];
      break;
    default:

    }
    return c;
  }
}
 * 
 * The real case may be more complicated than the primitive-based fields, and more 
 * inner classes generated recursively.
 */
abstract class CGField[+T <: ObjectInspector](
    val oi: T,
    val oiName: String,
    val clazz: String,
    val primitive: String,
    val order: Int) {

  def this(oi: T, name: String, clz: String, order: Int) {
    this(oi, name, clz, clz, order)
  }
  var typeInfo: TypeInfo = TIF.voidTypeInfo
  var writable = clazz
  
  lazy val maskBitName = CGField.getMaskBitVariableName(name)
  lazy val fieldValidity = CGField.getFieldValidity(name)
  // name is the escaped variable name
  lazy val name = CGUtil.makeCGFieldName(oiName)
  lazy val constant = oi.isInstanceOf[ConstantObjectInspector]
  lazy val constantValue = oi.asInstanceOf[ConstantObjectInspector].getWritableConstantValue()
  lazy val constantNull = constant && (constantValue == null)

  lazy val fullClassName: String = dynamicFullClassName.replace('$', '.')
  lazy val parentFullClassName: String = 
    if (dynamicParentFullClassName == null) null else dynamicParentFullClassName.replace('$', '.')
  
  // the dymamic class name is used for creating instance with class loader
  // e.g. Class.forName(xxx) or ClassLoader.loadClass(xxx), 
  // which requires the inner class name split with '$'
  lazy val dynamicFullClassName: String = dynamicParentFullClassName
  lazy val dynamicParentFullClassName: String = 
    if (parent != null) parent.dynamicFullClassName else null

  var parent: CGField[_] = _

  //def the static classes / blocks / methods
  lazy val defStaticBlocks: String = CGField.defStaticBlocks(this)

  // define the field
  lazy val defField = CGField.defField(this)

  // field de-serialization in the KryoSerializable.read
  lazy val defRead: String = CGField.defReads(this)

  // field serialization in the KryoSerializable.write
  lazy val defWrite: String = CGField.defWrites(this)

  // snippet for calculating the hashCode
  lazy val defHashCode = CGField.defHashCodes(this)

  /* the constant field value, Primitive Type will override the implementations, and the other
   * types may need static function(s) to do the real value extraction, which defined 
   * in the static block (implementing the function defStaticBlocks). 
   */
  lazy val defFieldValue = CGField.defFieldValue(this)

  // factory method to create instances of (struct / union)
  def defExtractValueViaOI(obj: String, varoi: String, data: String): String = 
    CGField.defExtractValueViaOIs(obj, varoi, data, this)

  // snippet for generating the equals
  def defEquals(thatVariable: String) = CGField.defEqualss(thatVariable, this)

  // define the expression for assign value to variable with concrete type, most likely to be the 
  // data type converting
  def defAssignValue(obj: String, data: String): String = CGField.defAssignValues(obj, data, this)

  /*
   * The idea of pass the constant value to the generated object can divide into the following 
   * steps:
   * a. Extract the value from the constant object inspector recursively
   *    1. Primitive ObjectInspector
   *       (bytes, boolean, byte, short, int, float, double, string, date, timestamp)
   *    2. List   (container) Array[v1, v2, v3...]
   *    3. Map    (container) Array[k1, v1, k2, v2, k3, v3...]
   *    4. Struct (container) Array[field1, field2, field3...]
   *    5. Union  (container) Array[tag, value] 
   *    And per the definition of CGRow, the Struct should be the top level object
   * b. Serialize the value (the Struct, which should be the array[....]) into byte array (via kryo)
   * c. Transform the byte array into hex and represented as string (package.scala 
   *      implicit def bytesConvert2HexString(bytes: Array[Byte]): String)
   * d. Put the string into the generated source, and while initiating the class(of the generated
   *    source), the constant string will be loaded as byte array naturely supported by JVM, and 
   *    then de-serialize the value from byte array
   * For example:
   * class Struct1 {
   *    public static final transient List<Integer> ll = INITIAL_ll();
   *    ......
   *    private static final List<Integer> INITIAL_LL() {
   *       // the {0x1, 0x2, 0xef, 0xa2} was generated by step a, b, c.
   *       byte[] arrays = new bytes[]{0x1,0x2,0xef,0xa2...};
   *       return (List<Integer>)(CGRowUtil.deserialize(arrays));
   *    }
   *    ......
   * }
   * 
   * The function transforms the constant value (can be read via object inspector) to value 
   * in java primitive types(numbers, byte(s), array, see step a), which is easier for the object
   * serialization and de-serialization.
   */
  def dataConstantOI2Java(obj: Any): Any = if (obj == null) null else dataOI2Java(obj)
  
  def setFieldValidity(validity: Boolean) = CGField.setFieldValidity(this, validity)

  protected def dataOI2Java(obj: Any): Any

  /*
   * The serialized constant object in string format, to be used in the generated java source.
   */
  lazy val constantBytesInString: String = CGUtil.serialize(dataConstantOI2Java(constantValue))
  
  override def equals(obj: Any) = if(obj == null) 
    false 
  else if(!obj.isInstanceOf[CGField[_]]) {
    false
  } else {
    obj.asInstanceOf[CGField[_]].typeInfo == this.typeInfo
  }
}

abstract class CGPrimitive[+T <: PrimitiveObjectInspector](oi: T,
    name: String,
    clazz: String,
    primitive: String,
    order: Int) extends CGField(oi, name, clazz, primitive, order) {
  
  protected override def dataOI2Java(data: Any) = oi.getPrimitiveJavaObject(data)
}

class CGNull(oi: VoidObjectInspector, name: String, order: Int)
  extends CGField(oi, name, null, null, order) {
  typeInfo = TIF.voidTypeInfo
  writable = null
  
  protected override def dataOI2Java(data: Any) = null
}

class CGPrimitiveBinary(oi: BinaryObjectInspector, name: String, order: Int)
  extends CGPrimitive(oi, name, "byte[]", "byte[]", order) {
  typeInfo = TIF.binaryTypeInfo
  writable = classOf[BytesWritable].getCanonicalName()

  protected override def dataOI2Java(data: Any) = oi.getPrimitiveJavaObject(data)
}

class CGPrimitiveBoolean(oi: BooleanObjectInspector, name: String, order: Int)
  extends CGPrimitive(oi, name, "Boolean", "boolean", order) {
  typeInfo = TIF.booleanTypeInfo
  writable = classOf[BooleanWritable].getCanonicalName()
}

class CGPrimitiveByte(oi: ByteObjectInspector, name: String, order: Int)
  extends CGPrimitive(oi, name, "Byte", "byte", order) {
  typeInfo = TIF.byteTypeInfo
  writable = classOf[ByteWritable].getCanonicalName()
}

class CGPrimitiveDouble(oi: DoubleObjectInspector, name: String, order: Int)
  extends CGPrimitive(oi, name, "Double", "double", order) {
  typeInfo = TIF.doubleTypeInfo
  writable = classOf[DoubleWritable].getCanonicalName()
}

class CGPrimitiveFloat(oi: FloatObjectInspector, name: String, order: Int)
  extends CGPrimitive(oi, name, "Float", "float", order) {
  typeInfo = TIF.floatTypeInfo
  writable = classOf[FloatWritable].getCanonicalName()
}

class CGPrimitiveInt(oi: IntObjectInspector, name: String, order: Int)
  extends CGPrimitive(oi, name, "Integer", "int", order) {
  typeInfo = TIF.intTypeInfo
  writable = classOf[IntWritable].getCanonicalName()
}

class CGPrimitiveLong(oi: LongObjectInspector, name: String, order: Int)
  extends CGPrimitive(oi, name, "Long", "long", order) {
  typeInfo = TIF.longTypeInfo
  writable = classOf[LongWritable].getCanonicalName()
}

class CGPrimitiveShort(oi: ShortObjectInspector, name: String, order: Int)
  extends CGPrimitive(oi, name, "Short", "short", order) {
  typeInfo = TIF.shortTypeInfo
  writable = classOf[ShortWritable].getCanonicalName()
}

class CGPrimitiveString(oi: StringObjectInspector, name: String, order: Int)
  extends CGPrimitive(oi, name, "String", "String", order) {
  typeInfo = TIF.stringTypeInfo
  writable = classOf[Text].getCanonicalName()
}

class CGPrimitiveTimestamp(oi: TimestampObjectInspector, name: String, order: Int)
  extends CGPrimitive(oi, name, "java.sql.Timestamp", "java.sql.Timestamp", order) {
  typeInfo = TIF.timestampTypeInfo
  writable = classOf[TimestampWritable].getCanonicalName()
}

class CGMap(oi: MapObjectInspector,
    name: String,
    val key: CGField[_ <: ObjectInspector],
    val value: CGField[_ <: ObjectInspector],
    order: Int)
  extends CGField(oi, name, "java.util.HashMap<%s,%s>".format(key.clazz, value.clazz), order) {
  typeInfo = TIF.getMapTypeInfo(key.typeInfo, value.typeInfo)

  {
    key.parent = this
    value.parent = this
  }

  /* to array[k,v, k,v....]*/
  protected override def dataOI2Java(data: Any): Any = {
    val map = data.asInstanceOf[java.util.Map[_, _]]
    val array = ArrayBuffer[Any]()
    val it = map.entrySet().iterator
    while (it.hasNext()) {
      val entry = it.next()
      array += key.dataConstantOI2Java(entry.getKey())
      array += value.dataConstantOI2Java(entry.getValue())
    }
    array.toArray
  }
}

class CGList(oi: ListObjectInspector,
             name: String,
             val field: CGField[_ <: ObjectInspector],
             order: Int)
  extends CGField(oi, name, "java.util.ArrayList<%s>".format(field.clazz), order) {
  typeInfo = TIF.getListTypeInfo(field.typeInfo)

  { field.parent = this }

  /* to array[v,v,v..]*/
  protected override def dataOI2Java(data: Any): Any = {
    val list = data.asInstanceOf[java.util.List[_]]
    val array = ArrayBuffer[Any]()
    val it = list.iterator()
    while (it.hasNext()) {
      val entry = it.next()
      array += field.dataConstantOI2Java(entry)
    }
    array.toArray
  }
}

/**
 * Can not be constant or constant null, cause there is no constant object inspector for Struct, and
 * not necessary
 */
class CGStruct(oi: StructObjectInspector, name: String, 
    val fields: Array[CGField[_<:ObjectInspector]], order: Int)
  extends CGField(oi, name, CGUtil.randStructClassName(), order) {
  import scala.collection.JavaConversions._
  
  def getField(fieldName: String) = {
    fields.find(_.oiName == fieldName) match {
      case Some(x) => x
      case None => throw new CGAssertRuntimeException("Cannot find the field name[" + fieldName +"]")
    }
  }
  
  typeInfo = TIF.getStructTypeInfo(
    mutableSeqAsJavaList(fields.map(_.oiName)), 
    mutableSeqAsJavaList(fields.map(_.typeInfo)))

  { fields.foreach(_.parent = this) }

  override lazy val dynamicFullClassName: String = CGField.getDynamicFullClassName(this)

  val maskVariableName = CGField.STRUCT_MASK_VARIABLE_NAME
    
  def getMaskBitVariableName(idx: Int) = CGField.getMaskBitVariableName(this, idx)

  def getFieldVariableName(idx: Int) = CGField.getFieldVariableName(this, idx)

  def getFieldValidity(data: String, idx: Int) = CGField.getFieldValidity(this, data, idx)
  
  def setFieldValidity(data: String, idx: Int, result: Boolean = true) = 
    CGField.setFieldValidity(this, data, idx, result)

  /* to array[v,v,v...] */
  protected override def dataOI2Java(data: Any): Any = {
    val array = ArrayBuffer[Any]()
    for (f <- fields) {
      array += f.dataConstantOI2Java(oi.getStructFieldData(data, oi.getStructFieldRef(f.oiName)))
    }

    array.toArray
  }
}

/**
 * Can not be constant or constant null, cause there is no constant object inspector for Union, and
 * not necessary
 */
class CGUnion(oi: UnionObjectInspector, name: String, val fields: Array[CGField[_]], order: Int)
  extends CGField(oi, name, CGUtil.randUnionClassName(), order) {
  import scala.collection.JavaConversions._

  typeInfo = TIF.getUnionTypeInfo(asJavaList(fields.map(_.typeInfo)))

  { fields.foreach(_.parent = this) }

  lazy val bytes: String =
    if (constant) {
      assert(oi.isInstanceOf[StandardConstantListObjectInspector])
      CGUtil.serialize(oi)
    } else {
      null
    }

  override lazy val dynamicFullClassName: String = CGField.getDynamicFullClassName(this)

  val tagVariableName = CGField.UNION_TAG_VARIABLE_NAME

  def getFieldVariableName(idx: Int) = fields(idx).name

  def getFieldValidity() = CGField.getFieldValidity(this)

  /* [tag, object] */
  protected override def dataOI2Java(data: Any): Any = {
    val tag = oi.getTag(data)
    val obj = fields(tag).dataConstantOI2Java(oi.getField(data))
    if (obj != null) {
      val array = ArrayBuffer[Any]()
      array += tag
      array += obj
      array.toArray
    } else {
      null
    }
  }
}

object CGField {
  val PACKAGE_NAME: String = "shark.execution.cg.row"
  val STRUCT_MASK_VARIABLE_NAME = "mask"
  val UNION_TAG_VARIABLE_NAME = "tag"

  private val ORDER_P_BINARY = 0
  private val ORDER_P_BOOLEAN = 1
  private val ORDER_P_BYTE = 2
  private val ORDER_P_DATE = 3
  private val ORDER_P_DOUBLE = 4
  private val ORDER_P_FLOAT = 5
  private val ORDER_P_INT = 6
  private val ORDER_P_LONG = 7
  private val ORDER_P_SHORT = 8
  private val ORDER_P_STRING = 9
  private val ORDER_P_TIMESTAMP = 10
  private val ORDER_MAP = 11
  private val ORDER_LIST = 12
  private val ORDER_STRUCT = 13
  private val ORDER_UNION = 14

  private val READS = Array(
    "%s = input.readBytes(input.readInt());",
    "%s = input.readBoolean();",
    "%s = input.readByte();",
    "%s = new java.sql.Date(input.readLong());",
    "%s = input.readDouble();",
    "%s = input.readFloat();",
    "%s = input.readInt();",
    "%s = input.readLong();",
    "%s = input.readShort();",
    "%s = input.readString();",
    "%s = new java.sql.Timestamp(input.readLong()); %s.setNanos(input.readInt());",
    "%s = kryo.readObject(input, java.util.HashMap.class);",
    "%s = kryo.readObject(input, java.util.ArrayList.class);",
    "%s = kryo.readObject(input, %s.class);",
    "%s = kryo.readObject(input, %s.class);"
  )
  private val DEF_READS = Array[(CGField[_]) => String](
    (f) => { READS(ORDER_P_BINARY).format(f.name) },
    (f) => { READS(ORDER_P_BOOLEAN).format(f.name) },
    (f) => { READS(ORDER_P_BYTE).format(f.name) },
    (f) => { READS(ORDER_P_DATE).format(f.name) },
    (f) => { READS(ORDER_P_DOUBLE).format(f.name) },
    (f) => { READS(ORDER_P_FLOAT).format(f.name) },
    (f) => { READS(ORDER_P_INT).format(f.name) },
    (f) => { READS(ORDER_P_LONG).format(f.name) },
    (f) => { READS(ORDER_P_SHORT).format(f.name) },
    (f) => { READS(ORDER_P_STRING).format(f.name) },
    (f) => { READS(ORDER_P_TIMESTAMP).format(f.name, f.name) },
    (f) => { READS(ORDER_MAP).format(f.name) },
    (f) => { READS(ORDER_LIST).format(f.name) },
    (f) => { READS(ORDER_STRUCT).format(f.name, f.clazz) },
    (f) => { READS(ORDER_UNION).format(f.name, f.clazz) }
  )

  private val WRITES = Array(
    "output.writeInt(%s.length); output.writeBytes(%s);",
    "output.writeBoolean(%s);",
    "output.writeByte(%s);",
    "output.writeLong(%s.getTime());",
    "output.writeDouble(%s);",
    "output.writeFloat(%s);",
    "output.writeInt(%s);",
    "output.writeLong(%s);",
    "output.writeShort(%s);",
    "output.writeString(%s);",
    "output.writeLong(%s.getTime()); output.writeInt(%s.getNanos());",
    "kryo.writeObjectOrNull(output, %s, java.util.HashMap.class);",
    "kryo.writeObjectOrNull(output, %s, java.util.ArrayList.class);",
    "kryo.writeObjectOrNull(output, %s, %s.class);",
    "kryo.writeObjectOrNull(output, %s, %s.class);"
  )

  private val DEF_WRITES = Array[(CGField[_]) => String](
    (f) => { WRITES(ORDER_P_BINARY).format(f.name, f.name) },
    (f) => { WRITES(ORDER_P_BOOLEAN).format(f.name) },
    (f) => { WRITES(ORDER_P_BYTE).format(f.name) },
    (f) => { WRITES(ORDER_P_DATE).format(f.name) },
    (f) => { WRITES(ORDER_P_DOUBLE).format(f.name) },
    (f) => { WRITES(ORDER_P_FLOAT).format(f.name) },
    (f) => { WRITES(ORDER_P_INT).format(f.name) },
    (f) => { WRITES(ORDER_P_LONG).format(f.name) },
    (f) => { WRITES(ORDER_P_SHORT).format(f.name) },
    (f) => { WRITES(ORDER_P_STRING).format(f.name) },
    (f) => { WRITES(ORDER_P_TIMESTAMP).format(f.name, f.name) },
    (f) => { WRITES(ORDER_MAP).format(f.name) },
    (f) => { WRITES(ORDER_LIST).format(f.name) },
    (f) => { WRITES(ORDER_STRUCT).format(f.name, f.clazz) },
    (f) => { WRITES(ORDER_UNION).format(f.name, f.clazz) }
  )

  private val HASHCODES = Array(
    "org.apache.commons.lang.ArrayUtils.hashCode(%s)",
    "org.apache.commons.lang.ObjectUtils.hashCode(%s)",
    "org.apache.commons.lang.ObjectUtils.hashCode(%s)",
    "org.apache.commons.lang.ObjectUtils.hashCode(%s)",
    "org.apache.commons.lang.ObjectUtils.hashCode(%s)",
    "org.apache.commons.lang.ObjectUtils.hashCode(%s)",
    "org.apache.commons.lang.ObjectUtils.hashCode(%s)",
    "org.apache.commons.lang.ObjectUtils.hashCode(%s)",
    "org.apache.commons.lang.ObjectUtils.hashCode(%s)",
    "org.apache.commons.lang.ObjectUtils.hashCode(%s)",
    "org.apache.commons.lang.ObjectUtils.hashCode(%s)",
    "(%s.hashCode())",
    "(%s.hashCode())",
    "(%s.hashCode())",
    "(%s.hashCode())"
  )

  private val EQUALS = Array(
    "org.apache.commons.lang.ArrayUtils.isEquals(%s, %s.%s)",
    "org.apache.commons.lang.ObjectUtils.equals(%s, %s.%s)",
    "org.apache.commons.lang.ObjectUtils.equals(%s, %s.%s)",
    "org.apache.commons.lang.ObjectUtils.equals(%s, %s.%s)",
    "org.apache.commons.lang.ObjectUtils.equals(%s, %s.%s)",
    "org.apache.commons.lang.ObjectUtils.equals(%s, %s.%s)",
    "org.apache.commons.lang.ObjectUtils.equals(%s, %s.%s)",
    "org.apache.commons.lang.ObjectUtils.equals(%s, %s.%s)",
    "org.apache.commons.lang.ObjectUtils.equals(%s, %s.%s)",
    "org.apache.commons.lang.ObjectUtils.equals(%s, %s.%s)",
    "org.apache.commons.lang.ObjectUtils.equals(%s, %s.%s)",
    "(%s.equals(%s.%s))",
    "(%s.equals(%s.%s))",
    "(%s.equals(%s.%s))",
    "(%s.equals(%s.%s))"
  )

  private val DEF_ASSIGN_VALUES = Array[(String, String, CGField[_]) => String](
    (obj, data, f) => { "%s = (byte[])%s;".format(obj, data) },
    (obj, data, f) => { "%s = (Boolean)%s;".format(obj, data) },
    (obj, data, f) => { "%s = (Byte)%s;".format(obj, data) },
    (obj, data, f) => { "%s = (java.sql.Date)%s;".format(obj, data) },
    (obj, data, f) => { "%s = (Double)%s;".format(obj, data) },
    (obj, data, f) => { "%s = (Float)%s;".format(obj, data) },
    (obj, data, f) => { "%s = (Integer)%s;".format(obj, data) },
    (obj, data, f) => { "%s = (Long)%s;".format(obj, data) },
    (obj, data, f) => { "%s = (Short)%s;".format(obj, data) },
    (obj, data, f) => { "%s = (String)%s;".format(obj, data) },
    (obj, data, f) => { "%s = (java.sql.Timestamp)%s;".format(obj, data) },
    (obj, data, f) => { "%s = %s.INITIAL_%s(%s);".format(obj, f.fullClassName, f.name, data) },
    (obj, data, f) => { "%s = %s.INITIAL_%s(%s);".format(obj, f.fullClassName, f.name, data) },
    (obj, data, f) => { "%s = %s.INITIAL(%s);".format(obj, f.fullClassName, data) },
    (obj, data, f) => { "%s = %s.INITIAL(%s);".format(obj, f.fullClassName, data) }
  )

  private val DEF_EXTRACT_VALUE_VIA_OI = 
    Array[(String, String, String, CGField[_]) => String](
    (obj, varoi, data, f) => { ("%s = ((org.apache.hadoop.hive.serde2.objectinspector.primitive." +
      "BinaryObjectInspector)%s).getPrimitiveJavaObject(%s).getData();").format(obj, varoi, data) },
    (obj, varoi, data, f) => { ("%s = ((org.apache.hadoop.hive.serde2.objectinspector.primitive." +
      "BooleanObjectInspector)%s).get(%s);").format(obj, varoi, data) },
    (obj, varoi, data, f) => { ("%s = ((org.apache.hadoop.hive.serde2.objectinspector.primitive." +
      "ByteObjectInspector)%s).get(%s);").format(obj, varoi, data) },
    (obj, varoi, data, f) => { ("%s = ((org.apache.hadoop.hive.serde2.objectinspector.primitive." +
      "DateObjectInspector)%s).getPrimitiveJavaObject(%s);").format(obj, varoi, data) },
    (obj, varoi, data, f) => { ("%s = ((org.apache.hadoop.hive.serde2.objectinspector.primitive." +
      "DoubleObjectInspector)%s).get(%s);").format(obj, varoi, data) },
    (obj, varoi, data, f) => { ("%s = ((org.apache.hadoop.hive.serde2.objectinspector.primitive." +
      "FloatObjectInspector)%s).get(%s);").format(obj, varoi, data) },
    (obj, varoi, data, f) => { ("%s = ((org.apache.hadoop.hive.serde2.objectinspector.primitive." +
      "IntObjectInspector)%s).get(%s);").format(obj, varoi, data) },
    (obj, varoi, data, f) => { ("%s = ((org.apache.hadoop.hive.serde2.objectinspector.primitive." +
      "LongObjectInspector)%s).get(%s);").format(obj, varoi, data) },
    (obj, varoi, data, f) => { ("%s = ((org.apache.hadoop.hive.serde2.objectinspector.primitive." +
      "ShortObjectInspector)%s).get(%s);").format(obj, varoi, data) },
    (obj, varoi, data, f) => { ("%s = ((org.apache.hadoop.hive.serde2.objectinspector.primitive." +
      "StringObjectInspector)%s).getPrimitiveJavaObject(%s);").format(obj, varoi, data) },
    (obj, varoi, data, f) => { ("%s = ((org.apache.hadoop.hive.serde2.objectinspector.primitive." +
      "TimestampObjectInspector)%s).getPrimitiveJavaObject(%s);").format(obj, varoi, data) },
    (obj, varoi, data, f) => { 
      "%s = %s.BUILD_%s(%s, %s);".format(obj, f.fullClassName, f.name, varoi, data) },
    (obj, varoi, data, f) => { 
      "%s = %s.BUILD_%s(%s, %s);".format(obj, f.fullClassName, f.name, varoi, data) },
    (obj, varoi, data, f) => { 
      "%s = %s.BUILD(%s, %s);".format(obj, f.fullClassName, varoi, data) },
    (obj, varoi, data, f) => { 
      "%s = %s.BUILD(%s, %s);".format(obj, f.fullClassName, varoi, data) }
  )

  private val FIELD_DEF_STATIC_NULL = "public static final transient %s %s;"
  private val FIELD_DEF_STATIC_VALUE = "public static final transient %s %s = %s;"
  private val FIELD_DEF = "public %s %s;"

  private val DEF_FIELD_VALUES = Array[(CGField[_]) => String](
    (f) => { f.oi.asInstanceOf[WritableConstantBinaryObjectInspector].getWritableConstantValue() },
    (f) => { f.oi.asInstanceOf[WritableConstantBooleanObjectInspector].getWritableConstantValue() },
    (f) => { f.oi.asInstanceOf[WritableConstantByteObjectInspector].getWritableConstantValue() },
    (f) => { f.oi.asInstanceOf[WritableConstantDoubleObjectInspector].getWritableConstantValue() },
    (f) => { f.oi.asInstanceOf[WritableConstantFloatObjectInspector].getWritableConstantValue() },
    (f) => { f.oi.asInstanceOf[WritableConstantIntObjectInspector].getWritableConstantValue() },
    (f) => { f.oi.asInstanceOf[WritableConstantLongObjectInspector].getWritableConstantValue() },
    (f) => { f.oi.asInstanceOf[WritableConstantShortObjectInspector].getWritableConstantValue() },
    (f) => { f.oi.asInstanceOf[WritableConstantStringObjectInspector].getWritableConstantValue() },
    (f) => { f.oi.asInstanceOf[WritableConstantTimestampObjectInspector].getWritableConstantValue() },
    (f) => { "INITIAL_%s()".format(f.name) },
    (f) => { "INITIAL_%s()".format(f.name) },
    (f) => { "INITIAL_%s()".format(f.name) },
    (f) => { "INITIAL_%s()".format(f.name) }
  )

  private val DEF_STATIC_BLOCK = Array[(AnyRef) => String](
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
    (f) => { CGTE.layout(CGRow.CG_ROW_OI_2_MAP, Map("obj" -> f)) },
    (f) => { CGTE.layout(CGRow.CG_ROW_OI_2_LIST, Map("obj" -> f)) },
    (f) => { CGTE.layout(CGRow.CG_ROW_CLASS_STRUCT, Map("isOutter" -> false, "struct" -> f)) },
    (f) => { CGTE.layout(CGRow.CG_ROW_CLASS_UNION, Map("obj" -> f)) }
  )

  def defReads(f: CGField[_]) = DEF_READS(f.order)(f)
  
  def defWrites(f: CGField[_]) = DEF_WRITES(f.order)(f)
  
  def defHashCodes(f: CGField[_]) = HASHCODES(f.order).format(f.name)
  
  def defEqualss(that: String, f: CGField[_]) = 
    EQUALS(f.order).format(f.name, that, f.name)
  
  def defAssignValues(obj: String, data: String, f: CGField[_]) = 
    DEF_ASSIGN_VALUES(f.order)(obj, data, f)
    
  def defExtractValueViaOIs(obj: String, varoi: String, data: String, f: CGField[_]) = 
    DEF_EXTRACT_VALUE_VIA_OI(f.order)(obj, varoi, data, f)
    
  def defField(f: CGField[_]) = if (f.constant) {
    FIELD_DEF_STATIC_VALUE.format(f.primitive, f.name, f.defFieldValue)
  } else {
    FIELD_DEF.format(f.primitive, f.name)
  }

  def defFieldValue(f: CGField[_]) =
    if (f.constantNull && !f.isInstanceOf[CGPrimitive[_]])
      "null"
    else
      DEF_FIELD_VALUES(f.order)(f)

  def defStaticBlocks(f: CGField[_]) = DEF_STATIC_BLOCK(f.order)(f)

  def create(oi: StructObjectInspector): CGStruct = {
    var struct = create(oi, CGUtil.randStructClassName()).asInstanceOf[CGStruct]

    struct
  }

  def getDynamicFullClassName(struct: CGStruct) = {
    if (null == struct.dynamicParentFullClassName) {
      "%s.%s".format(CGField.PACKAGE_NAME, struct.clazz)
    } else {
      "%s$%s".format(struct.dynamicParentFullClassName, struct.clazz)
    }
  }
  
  def getDynamicFullClassName(union: CGUnion) = {
    "%s$%s".format(union.dynamicParentFullClassName, union.clazz)
  }
  
  def getMaskBitVariableName(name: String): String = 
    "MASK_%s".format(name.toUpperCase())
    
  def getMaskBitVariableName(struct: CGStruct, idx: Int): String = 
    "%s.%s".format(struct.fullClassName, getMaskBitVariableName(struct.fields(idx).name))
    
  def getFieldValidity(name: String): String = 
    "%s.get(%s)".format(CGField.STRUCT_MASK_VARIABLE_NAME, getMaskBitVariableName(name))
  
  def getFieldValidity(struct: CGStruct, data: String, idx: Int): String = 
    "%s.%s".format(data, getFieldValidity(struct.getMaskBitVariableName(idx)))

  def getFieldValidity(union: CGUnion) = 
    "%s.%s < 0".format(union.name, union.tagVariableName)
    
  def setFieldValidity(field: CGField[_], result: Boolean): String = 
    "%s.set(%s.%s, %s)".format(CGField.STRUCT_MASK_VARIABLE_NAME, 
      field.parentFullClassName, field.maskBitName, result)
  
  def setFieldValidity(struct: CGStruct, data: String, idx: Int, result: Boolean): String = 
    "%s.%s".format(data, setFieldValidity(struct.fields(idx), result))

  def getFieldVariableName(struct: CGStruct, idx: Int) = 
    "%s.%s".format(struct.name, struct.fields(idx).name)

  def create(oi: ObjectInspector, name: String): CGField[_ <: ObjectInspector] = {
    import collection.JavaConversions._
    oi match {
      case a: StructObjectInspector =>
        new CGStruct(a,
          name,
          Array.tabulate[CGField[_ <: ObjectInspector]](a.getAllStructFieldRefs().size()) { i =>
            var foi = a.getAllStructFieldRefs()(i)
            create(foi.getFieldObjectInspector(), foi.getFieldName())
          },
          ORDER_STRUCT
        )
      case a: ListObjectInspector =>
        new CGList(a,
          name,
          create(a.getListElementObjectInspector(), "%s_l".format(name)), // default name
          ORDER_LIST)
      case a: MapObjectInspector =>
        new CGMap(a,
          name,
          create(a.getMapKeyObjectInspector(), "%s_k".format(name)), // default name
          create(a.getMapValueObjectInspector(), "%s_v".format(name)), // default name
          ORDER_MAP)
      case a: UnionObjectInspector =>
        new CGUnion(a, name,
          Array.tabulate[CGField[_]](a.getObjectInspectors().size()) { i =>
            create(a.getObjectInspectors()(i), "%s_%d".format(name, i)) // default name
          },
          ORDER_UNION
        )
      case x: BinaryObjectInspector    => new CGPrimitiveBinary(x, name, ORDER_P_BINARY)
      case x: BooleanObjectInspector   => new CGPrimitiveBoolean(x, name, ORDER_P_BOOLEAN)
      case x: ByteObjectInspector      => new CGPrimitiveByte(x, name, ORDER_P_BYTE)
      case x: DoubleObjectInspector    => new CGPrimitiveDouble(x, name, ORDER_P_DOUBLE)
      case x: FloatObjectInspector     => new CGPrimitiveFloat(x, name, ORDER_P_FLOAT)
      case x: IntObjectInspector       => new CGPrimitiveInt(x, name, ORDER_P_INT)
      case x: LongObjectInspector      => new CGPrimitiveLong(x, name, ORDER_P_LONG)
      case x: ShortObjectInspector     => new CGPrimitiveShort(x, name, ORDER_P_SHORT)
      case x: StringObjectInspector    => new CGPrimitiveString(x, name, ORDER_P_STRING)
      case x: TimestampObjectInspector => new CGPrimitiveTimestamp(x, name, ORDER_P_TIMESTAMP)
      case x: Any => throw new CGAssertRuntimeException("couldn't find " + x.getClass())
    }
  }
}

object CGTE {
  val engine = new TemplateEngine
  engine.allowReload = false
  engine.allowCaching = true

  def layout(ssp: String, map: Map[String, Any]) = {
    engine.layout(ssp, map)
  }
}

object CGRow {
  val CG_ROW_CLASS_STRUCT = "shark/execution/cg/row/cg_class_struct.ssp"
  val CG_ROW_CLASS_UNION = "shark/execution/cg/row/cg_class_union.ssp"
  val CG_ROW_OI_2_LIST = "shark/execution/cg/row/cg_oi_2_list.ssp"
  val CG_ROW_OI_2_MAP = "shark/execution/cg/row/cg_oi_2_map.ssp"

  def generate(struct: CGStruct, isOutter: Boolean = false): String = {
    CGTE.layout(CGRow.CG_ROW_CLASS_STRUCT, Map("isOutter" -> isOutter, "struct" -> struct))
  }
}
