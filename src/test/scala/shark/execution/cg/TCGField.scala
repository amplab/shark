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

package shark.execution.cg


import java.sql.Date
import java.sql.Timestamp
import java.util.{ Map, HashMap, List, ArrayList }
import java.util.BitSet
import javax.naming.OperationNotSupportedException

import scala.collection.mutable.ArrayBuffer

import org.apache.commons.lang.ArrayUtils

import shark.execution.cg.row.CGField
import shark.execution.cg.row.CGPrimitive
import shark.execution.cg.row.CGStruct
import shark.execution.cg.row.CGUnion
import shark.execution.cg.row.CGMap
import shark.execution.cg.row.CGList

import org.apache.hadoop.io.BytesWritable
import org.apache.hadoop.io.BooleanWritable
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.FloatWritable
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.hive.serde2.io.DoubleWritable
import org.apache.hadoop.hive.serde2.io.TimestampWritable
import org.apache.hadoop.hive.serde2.io.ByteWritable
import org.apache.hadoop.hive.serde2.io.ShortWritable

import shark.execution.serialization.KryoSerializer
import shark.execution.cg.CGUtil

import org.apache.hadoop.hive.serde2.columnar.ColumnarStructBase
import org.apache.hadoop.hive.serde2.columnar.ColumnarStruct
import org.apache.hadoop.hive.serde2.`lazy`.LazyObjectBase
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ByteObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ShortObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.StandardConstantMapObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.StandardUnionObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.{ ObjectInspector => OI }
import org.apache.hadoop.hive.serde2.objectinspector.{ ObjectInspectorFactory => OIF }
import org.apache.hadoop.hive.serde2.objectinspector.primitive.{ 
  PrimitiveObjectInspectorFactory => POIF }
import org.apache.hadoop.hive.serde2.objectinspector.{ StructObjectInspector => SOI }
import org.apache.hadoop.hive.serde2.objectinspector.{ UnionObjectInspector => UOI }
import org.apache.hadoop.hive.serde2.objectinspector.{ ListObjectInspector => LOI }
import org.apache.hadoop.hive.serde2.objectinspector.{ MapObjectInspector => MOI }
import org.apache.hadoop.hive.serde2.objectinspector.{ PrimitiveObjectInspector => POI }

object EnumConstant {
  val NOT_CONST: Int = 0
  val CONST: Int = 1
}

abstract class TCGField[+T](val ec: Int, val oiName: String, val ref: T) {
  lazy val name = CGUtil.makeCGFieldName(oiName)
  
  // for Hive ObjectInspectors value
  def createConstValue(): AnyRef
  def createOI(): OI

  // for generated cgrow value
  def feedCG(cgfield: CGField[_]): AnyRef

  // compare the result
  def compare(data: AnyRef)

  // compare the result which extracted by ObjectInspector
  def compare(data: AnyRef, oi: OI)

  // writable to java object
  def extract(data: AnyRef, oi: OI): AnyRef = {
    oi match {
      case x: POI => x.getPrimitiveJavaObject(data)
      case x: SOI => data
      case x: UOI => data
      case x: MOI => {
        val map = x.getMap(data).asInstanceOf[java.util.Map[AnyRef, AnyRef]]
        if (map == null) {
          null
        } else {
          val result = new java.util.HashMap[AnyRef, AnyRef]()
          val it = map.entrySet().iterator()
          while (it.hasNext()) {
            val entry = it.next()
            result.put(extract(entry.getKey(), x.getMapKeyObjectInspector()),
              extract(entry.getValue(), x.getMapValueObjectInspector()))
          }

          result
        }
      }
      case x: LOI => {
        val list = x.getList(data).asInstanceOf[java.util.List[AnyRef]]
        if (list == null) {
          null
        } else {
          val result = new java.util.ArrayList[AnyRef]()
          val it = list.iterator()
          while (it.hasNext) {
            val entry = it.next()
            result.add(extract(entry, x.getListElementObjectInspector()))
          }

          result
        }
      }
    }
  }
}

abstract class TCGPrimitive[+T <: AnyRef](ec: Int, name: String, override val ref: T)
  extends TCGField[T](ec, name, ref) {

  override def compare(data: AnyRef) {
    assert(org.apache.commons.lang.ObjectUtils.equals(ref, data))
  }

  override def feedCG(cgfield: CGField[_]) = {
    assert(cgfield.isInstanceOf[CGPrimitive[_]])

    ref
  }
}

class TCGBinary(ec: Int = EnumConstant.NOT_CONST, ref: Array[Byte] = Array[Byte](0x1, 0x2, 0x3), name: String = "pbinary")
  extends TCGPrimitive(ec, name, ref) {

  override def createConstValue() = if (ref == null) null else new BytesWritable(ref)

  override def createOI(): OI = {
    if (ec == EnumConstant.CONST) {
      POIF.getPrimitiveWritableConstantObjectInspector(PrimitiveCategory.BINARY, createConstValue())
    } else {
      // not constant
      POIF.writableBinaryObjectInspector
    }
  }
  override def compare(data: AnyRef): Unit = assert(ArrayUtils.isEquals(ref, data))

  override def compare(data: AnyRef, oi: OI) {
    assert(oi.isInstanceOf[BinaryObjectInspector])
    if (ref == null) {
      assert(oi.asInstanceOf[BinaryObjectInspector].getPrimitiveJavaObject(data) == null)
    } else {
      val d = oi.asInstanceOf[BinaryObjectInspector].getPrimitiveWritableObject(data)
      assert(ref.length == d.getSize())
      for (i <- 0 until ref.length) assert(ref(i) == d.getBytes()(i))
    }
  }
}

class TCGBoolean(ec: Int = EnumConstant.NOT_CONST, ref: java.lang.Boolean = true, name: String = "pboolean")
  extends TCGPrimitive(ec, name, ref) {

  override def createConstValue() = if (ref == null) null else new BooleanWritable(ref)

  override def createOI(): OI = {
    if (ec == EnumConstant.CONST) {
      POIF.getPrimitiveWritableConstantObjectInspector(PrimitiveCategory.BOOLEAN, createConstValue())
    } else {
      // not constant
      POIF.writableBooleanObjectInspector
    }
  }

  override def compare(data: AnyRef, oi: OI) {
    assert(oi.isInstanceOf[BooleanObjectInspector])
    if (ref == null) {
      assert(oi.asInstanceOf[BooleanObjectInspector].getPrimitiveWritableObject(data) == null)
    } else {
      assert(org.apache.commons.lang.ObjectUtils.equals(ref, oi.asInstanceOf[BooleanObjectInspector].getPrimitiveJavaObject(data)))
    }
  }
}

class TCGByte(ec: Int = EnumConstant.NOT_CONST, ref: java.lang.Byte = 1.asInstanceOf[Byte], name: String = "pbyte")
  extends TCGPrimitive(ec, name, ref) {

  override def createConstValue() = if (ref == null) null else new ByteWritable(ref)

  override def createOI(): OI = {
    if (ec == EnumConstant.CONST) {
      POIF.getPrimitiveWritableConstantObjectInspector(PrimitiveCategory.BYTE, createConstValue())
    } else {
      // not constant
      POIF.writableByteObjectInspector
    }
  }

  override def compare(data: AnyRef, oi: OI) {
    assert(oi.isInstanceOf[ByteObjectInspector])
    if (ref == null) {
      assert(oi.asInstanceOf[ByteObjectInspector].getPrimitiveJavaObject(data) == null)
    } else {
      assert(org.apache.commons.lang.ObjectUtils.equals(ref, oi.asInstanceOf[ByteObjectInspector].getPrimitiveJavaObject(data)))
    }
  }
}

class TCGShort(ec: Int = EnumConstant.NOT_CONST, ref: java.lang.Short = 2.asInstanceOf[Short], name: String = "pshort")
  extends TCGPrimitive(ec, name, ref) {

  override def createConstValue() = if (ref == null) null else new ShortWritable(ref)

  override def createOI(): OI = {
    if (ec == EnumConstant.CONST) {
      POIF.getPrimitiveWritableConstantObjectInspector(PrimitiveCategory.SHORT, createConstValue())
    } else {
      // not constant
      POIF.writableShortObjectInspector
    }
  }

  override def compare(data: AnyRef, oi: OI) {
    assert(oi.isInstanceOf[ShortObjectInspector])
    if (ref == null) {
      assert(oi.asInstanceOf[ShortObjectInspector].getPrimitiveJavaObject(data) == null)
    } else {
      assert(org.apache.commons.lang.ObjectUtils.equals(ref, oi.asInstanceOf[ShortObjectInspector].getPrimitiveJavaObject(data)))
    }
  }
}

class TCGInt(ec: Int = EnumConstant.NOT_CONST, ref: java.lang.Integer = 0x3, name: String = "pint")
  extends TCGPrimitive(ec, name, ref) {

  override def createConstValue() = if (ref == null) null else new IntWritable(ref)

  override def createOI(): OI = {
    if (ec == EnumConstant.CONST) {
      POIF.getPrimitiveWritableConstantObjectInspector(PrimitiveCategory.INT, createConstValue())
    } else {
      // not constant
      POIF.writableIntObjectInspector
    }
  }

  override def compare(data: AnyRef, oi: OI) {
    assert(oi.isInstanceOf[IntObjectInspector])
    if (ref == null) {
      assert(oi.asInstanceOf[IntObjectInspector].getPrimitiveJavaObject(data) == null)
    } else {
      assert(org.apache.commons.lang.ObjectUtils.equals(ref, oi.asInstanceOf[IntObjectInspector].getPrimitiveJavaObject(data)))
    }
  }
}

class TCGFloat(ec: Int = EnumConstant.NOT_CONST, ref: java.lang.Float = 4.1.asInstanceOf[Float], name: String = "pfloat")
  extends TCGPrimitive(ec, name, ref) {

  override def createConstValue() = if (ref == null) null else new FloatWritable(ref)

  override def createOI(): OI = {
    if (ec == EnumConstant.CONST) {
      POIF.getPrimitiveWritableConstantObjectInspector(PrimitiveCategory.FLOAT, createConstValue())
    } else {
      // not constant
      POIF.writableFloatObjectInspector
    }
  }
  override def compare(data: AnyRef, oi: OI) {
    assert(oi.isInstanceOf[FloatObjectInspector])
    if (ref == null) {
      assert(oi.asInstanceOf[FloatObjectInspector].getPrimitiveJavaObject(data) == null)
    } else {
      assert(org.apache.commons.lang.ObjectUtils.equals(ref, oi.asInstanceOf[FloatObjectInspector].getPrimitiveJavaObject(data)))
    }
  }
}

class TCGDouble(ec: Int = EnumConstant.NOT_CONST, ref: java.lang.Double = 5.1.asInstanceOf[Double], name: String = "pdouble")
  extends TCGPrimitive(ec, name, ref) {

  override def createConstValue() = if (ref == null) null else new DoubleWritable(ref)

  override def createOI(): OI = {
    if (ec == EnumConstant.CONST) {
      POIF.getPrimitiveWritableConstantObjectInspector(PrimitiveCategory.DOUBLE, createConstValue())
    } else {
      // not constant
      POIF.writableDoubleObjectInspector
    }
  }
  override def compare(data: AnyRef, oi: OI) {
    assert(oi.isInstanceOf[DoubleObjectInspector])
    if (ref == null) {
      assert(oi.asInstanceOf[DoubleObjectInspector].getPrimitiveJavaObject(data) == null)
    } else {
      assert(org.apache.commons.lang.ObjectUtils.equals(ref, oi.asInstanceOf[DoubleObjectInspector].getPrimitiveJavaObject(data)))
    }
  }
}

class TCGLong(ec: Int = EnumConstant.NOT_CONST, ref: java.lang.Long = 6.asInstanceOf[Long], name: String = "plong")
  extends TCGPrimitive(ec, name, ref) {

  override def createConstValue() = if (ref == null) null else new LongWritable(ref)

  override def createOI(): OI = {
    if (ec == EnumConstant.CONST) {
      POIF.getPrimitiveWritableConstantObjectInspector(PrimitiveCategory.LONG, createConstValue())
    } else {
      // not constant
      POIF.writableLongObjectInspector
    }
  }
  override def compare(data: AnyRef, oi: OI) {
    assert(oi.isInstanceOf[LongObjectInspector])
    if (ref == null) {
      assert(oi.asInstanceOf[LongObjectInspector].getPrimitiveJavaObject(data) == null)
    } else {
      assert(org.apache.commons.lang.ObjectUtils.equals(ref, oi.asInstanceOf[LongObjectInspector].getPrimitiveJavaObject(data)))
    }
  }
}

class TCGString(ec: Int = EnumConstant.NOT_CONST, ref: String = "\"!@#$%^&*()'", name: String = "pstring")
  extends TCGPrimitive(ec, name, ref) {
  override def createConstValue() = if (ref == null) null else new Text(ref)

  override def createOI(): OI = {
    if (ec == EnumConstant.CONST) {
      POIF.getPrimitiveWritableConstantObjectInspector(PrimitiveCategory.STRING, createConstValue())
    } else {
      // not constant
      POIF.writableStringObjectInspector
    }
  }

  override def compare(data: AnyRef, oi: OI) {
    assert(oi.isInstanceOf[StringObjectInspector])
    if (ref == null) {
      assert(oi.asInstanceOf[StringObjectInspector].getPrimitiveJavaObject(data) == null)
    } else {
      assert(org.apache.commons.lang.ObjectUtils.equals(ref, oi.asInstanceOf[StringObjectInspector].getPrimitiveJavaObject(data)))
    }
  }
}

class TCGTimestamp(ec: Int = EnumConstant.NOT_CONST, ref: Timestamp = new Timestamp(12345689l), name: String = "ptimestamp")
  extends TCGPrimitive(ec, name, ref) {

  override def createConstValue() = if (ref == null) null else new TimestampWritable(ref)

  override def createOI(): OI = {
    if (ec == EnumConstant.CONST) {
      POIF.getPrimitiveWritableConstantObjectInspector(PrimitiveCategory.TIMESTAMP, createConstValue())
    } else {
      // not constant
      POIF.writableTimestampObjectInspector
    }
  }

  override def compare(data: AnyRef, oi: OI) {
    assert(oi.isInstanceOf[TimestampObjectInspector])
    if (ref == null) {
      assert(oi.asInstanceOf[TimestampObjectInspector].getPrimitiveJavaObject(data) == null)
    } else {
      assert(org.apache.commons.lang.ObjectUtils.equals(ref, oi.asInstanceOf[TimestampObjectInspector].getPrimitiveJavaObject(data)))
    }
  }
}

// the TCGField requires the ref field as the val, we need additional object to wrap the ref
sealed class Struct(var data: AnyRef = null)
class TCGStruct(ec: Int, name: String = "ss", children: Array[TCGField[AnyRef]])
  extends TCGField[Struct](ec, name, new Struct()) {

  override def createConstValue(): ColumnarStructBase = {
    class ConstantColumnarStruct extends ColumnarStruct(createOI(), new ArrayList[java.lang.Integer](), null) {
      private val elements = new ArrayList[AnyRef]()
      children.foreach { f =>
        elements.add(f.createConstValue())
      }

      override def getField(fieldID: Int): AnyRef = {
        elements.get(fieldID)
      }

      override def createLazyObjectBase(oi: OI): LazyObjectBase = null
    }

    new ConstantColumnarStruct()
  }

  override def createOI(): OI = {
    assert(children != null && children.length > 0)

    import collection.JavaConversions.asJavaList

    OIF.getColumnarStructObjectInspector(
      asJavaList(children.map(_.oiName)), asJavaList((children.map(_.createOI())))
    )
  }

  override def compare(data: AnyRef) {
    assert(data == ref.data) // equals method in the generated source
    val mask = BeanPropertyHelper.getPropertyValue(data, CGField.STRUCT_MASK_VARIABLE_NAME).asInstanceOf[BitSet]

    for (i <- 0 until children.length) {
      val t = children(i)

      val maskbit = BeanPropertyHelper.getPropertyValue(data, CGField.getMaskBitVariableName(t.name))
        .asInstanceOf[java.lang.Integer]
      assert(maskbit >= 0)

      if (mask.get(maskbit)) {
        var value = BeanPropertyHelper.getPropertyValue(data, t.name)
        t.compare(value)
      } else {
        t.compare(null)
      }
    }
  }

  override def compare(data: AnyRef, oi: OI) {
    assert(oi.isInstanceOf[SOI])
    if (ref.data == null) {
      assert(data == null)
    } else {
      val soi = oi.asInstanceOf[SOI]
      val mask = BeanPropertyHelper.getPropertyValue(data, CGField.STRUCT_MASK_VARIABLE_NAME).asInstanceOf[BitSet]

      for (i <- 0 until children.length) {
        val t = children(i)

        val maskbit = BeanPropertyHelper.getPropertyValue(data, CGField.getMaskBitVariableName(t.name))
          .asInstanceOf[java.lang.Integer]
        assert(maskbit >= 0)

        val sf = soi.getStructFieldRef(t.oiName)
        if (mask.get(maskbit)) {
          t.compare(soi.getStructFieldData(data, sf), sf.getFieldObjectInspector())
        } else {
          t.compare(null, sf.getFieldObjectInspector())
        }
      }
    }
  }

  override def feedCG(cgfield: CGField[_]) = {
    assert(cgfield.isInstanceOf[CGStruct])
    val struct = cgfield.asInstanceOf[CGStruct]
    assert(children.length == struct.fields.length)

    ref.data = BeanPropertyHelper.instantiate(struct.dynamicFullClassName)
    val mask = BeanPropertyHelper.getPropertyValue(ref.data, CGField.STRUCT_MASK_VARIABLE_NAME).asInstanceOf[BitSet]

    for (i <- 0 until children.length) yield {
      val t = children(i)
      val c: CGField[_] = struct.fields(i)

      // mark the mask bit, to set as true
      val maskbit = i
      val subdata = t.feedCG(c)
      if (t.ec == EnumConstant.NOT_CONST) {
        if (subdata != null) {
          // if not null value, then set the property value
          BeanPropertyHelper.setPropertyValue(ref.data, c.name, subdata)
        }

        mask.set(maskbit, true)
      }
    }

    ref.data
  }
}

// the TCGField requires the ref field as the val, we need additional object to wrap the ref
sealed class Union(val tag: Byte, var data: AnyRef = null)
class TCGUnion(ec: Int, name: String = "uu", val children: Array[TCGField[AnyRef]], tag: Byte)
  extends TCGField[Union](ec, name, new Union(tag)) {

  var fieldName: String = _
  var tagName: String = _

  override def createConstValue(): StandardUnionObjectInspector.StandardUnion = {
    new StandardUnionObjectInspector.StandardUnion(
      ref.tag,
      (children(ref.tag)).createConstValue())
  }

  override def createOI(): OI = {
    assert(children != null && children.length > 0)

    import collection.JavaConversions.asJavaList
    OIF.getStandardUnionObjectInspector(
      asJavaList((children.map(_.createOI())))
    )
  }

  override def compare(data: AnyRef) {
    assert(ref.tag == BeanPropertyHelper.getPropertyValue(data, tagName))
    val field = children(ref.tag)

    assert(data == ref.data)

    val value = BeanPropertyHelper.getPropertyValue(data, fieldName)
    field.compare(value)
  }

  override def compare(data: AnyRef, oi: OI) {
    assert(oi.isInstanceOf[UOI])
    val uoi = oi.asInstanceOf[UOI]
    val field = children(ref.tag)
    val utag = uoi.getTag(data)
    assert(ref.tag == utag)
    field.compare(uoi.getField(data), uoi.getObjectInspectors().get(utag))
  }

  override def feedCG(cgfield: CGField[_]) = {
    assert(cgfield.isInstanceOf[CGUnion])
    val union = cgfield.asInstanceOf[CGUnion]
    assert(children.length == union.fields.length)

    ref.data = BeanPropertyHelper.instantiate(union.dynamicFullClassName)
    fieldName = union.getFieldVariableName(ref.tag)
    tagName = union.tagVariableName

    val c: CGField[_] = union.fields(ref.tag)
    val t = children(ref.tag)
    val subdata = t.feedCG(c)

    if (subdata != null) {
      if (t.ec == EnumConstant.NOT_CONST) {
        // if not null value, then set the property value
        BeanPropertyHelper.setPropertyValue(ref.data, fieldName, subdata)
      }
    }

    BeanPropertyHelper.setPropertyValue(ref.data, tagName, ref.tag)

    ref.data
  }
}

class TCGMap(t: Int, name: String = "mm", val keys: Array[TCGField[AnyRef]],
             val values: Array[TCGField[AnyRef]])
  extends TCGField[HashMap[AnyRef, AnyRef]](t, name, new HashMap[AnyRef, AnyRef]()) {

  override def createConstValue(): Map[AnyRef, AnyRef] = {
    val map = new HashMap[AnyRef, AnyRef]()
    for (i <- 0 until keys.length) yield {
      val key = keys(i)
      val value = values(i)
      map.put(key.createConstValue(), value.createConstValue())
    }

    map
  }

  override def createOI(): OI = {
    assert(keys != null && values != null && keys.length > 0)
    assert(keys.length == values.length)
    values.foldLeft(values(0).getClass())((clazz, field) => {
      assert(clazz == field.getClass())
      clazz
    }
    )

    keys.foldLeft(keys(0).getClass())((clazz, field) => {
      assert(clazz == field.getClass())
      clazz
    }
    )

    if (t == EnumConstant.CONST) {
      val map: HashMap[AnyRef, AnyRef] = new HashMap()
      for (i <- 0 until keys.length) yield {
        val key = keys(i)
        val value = values(i)
        map.put(key.createConstValue(), value.createConstValue())
      }
      OIF.getStandardConstantMapObjectInspector(keys(0).createOI(), values(0).createOI(), map)
    } else {
      OIF.getStandardMapObjectInspector((keys(0)).createOI(), ((values(0)).createOI()))
    }
  }

  override def compare(data: AnyRef) {
    assert(data.isInstanceOf[java.util.Map[AnyRef, AnyRef]])
    val map = data.asInstanceOf[java.util.Map[AnyRef, AnyRef]]
    assert(data == ref)

    for (i <- 0 until keys.length) {
      val key = keys(i)
      val value = values(i)

      val refvalue = map.get(key.ref).asInstanceOf[AnyRef]

      key.compare(key.ref)
      value.compare(refvalue)
    }
  }

  override def compare(data: AnyRef, oi: OI) {
    assert(oi.isInstanceOf[MOI])

    compare(extract(data, oi))
  }

  override def feedCG(cgfield: CGField[_]) = {
    assert(cgfield.isInstanceOf[CGMap])
    val map = cgfield.asInstanceOf[CGMap]

    for (i <- 0 until keys.length) yield {
      val k = keys(i)
      val v = values(i)
      ref.put(k.feedCG(map.key), v.feedCG(map.value))
    }

    ref
  }
}

class TCGList(t: Int, name: String = "ll", val values: Array[TCGField[AnyRef]])
  extends TCGField[ArrayList[AnyRef]](t, name, new ArrayList[AnyRef]()) {

  override def createConstValue(): List[AnyRef] = {
    val list = new ArrayList[AnyRef](values.length)
    values.foreach {
      (v =>
        list.add(v.createConstValue()))
    }

    list
  }

  override def createOI(): OI = {
    assert(values != null && values.length > 0)
    values.foldLeft(values(0).getClass())((clazz, field) => {
      assert(clazz == field.getClass())
      clazz
    }
    )

    if (t == EnumConstant.CONST) {
      val list: List[AnyRef] = new ArrayList()
      values.foreach(value => {
        list.add(value.createConstValue())
      })

      OIF.getStandardConstantListObjectInspector((values(0)).createOI(), list)
    } else {
      // not constant
      OIF.getStandardListObjectInspector((values(0)).createOI())
    }
  }

  override def compare(data: AnyRef) {
    assert(data.isInstanceOf[ArrayList[AnyRef]])
    val result = data.asInstanceOf[ArrayList[AnyRef]]
    assert(result == ref)
    val list = data.asInstanceOf[ArrayList[AnyRef]]
    for (i <- 0 until values.length) {
      values(i).compare(list.get(i))
    }
  }

  override def compare(data: AnyRef, oi: OI) {
    assert(oi.isInstanceOf[LOI])
    assert(data.isInstanceOf[ArrayList[AnyRef]])
    val loi = oi.asInstanceOf[LOI]
    val list = data.asInstanceOf[ArrayList[AnyRef]]
    for (i <- 0 until values.length) {
      values(i).compare(loi.getListElement(data, i), loi.getListElementObjectInspector())
    }
  }

  override def feedCG(cgfield: CGField[_]) = {
    assert(cgfield.isInstanceOf[CGList])

    val list = cgfield.asInstanceOf[CGList]

    for (value <- values) yield {
      val element = value.feedCG(list.field)
      ref.add(element)
    }

    ref
  }
}

