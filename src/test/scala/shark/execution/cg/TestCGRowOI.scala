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

import java.util.{ List => JList }
import java.util.BitSet
import java.sql.Date
import java.sql.Timestamp
import java.io.ByteArrayInputStream

import scala.collection.mutable.ListBuffer

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfterEach
import org.junit.Test
import org.junit.Before
import org.junit.After
import org.junit.Assert

import shark.execution.cg.row.CGField
import shark.execution.cg.row.CGRow
import shark.execution.cg.row.CGStruct
import shark.execution.cg.row.CGMap
import shark.execution.cg.row.CGOIField
import shark.execution.cg.row.CGOI
import shark.execution.cg.row.CGOIStruct
import shark.execution.serialization.KryoSerializer

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.Output
import com.esotericsoftware.kryo.io.Input

import org.apache.commons.beanutils.PropertyUtils
import org.apache.commons.io.output.ByteArrayOutputStream

import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory
import org.apache.hadoop.io.BytesWritable
import org.apache.hadoop.io.BooleanWritable
import org.apache.hadoop.io.FloatWritable
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory
import org.apache.hadoop.hive.serde2.objectinspector.{ ObjectInspectorFactory => OIF }
import org.apache.hadoop.hive.serde2.objectinspector.{ ObjectInspector => OI }
import org.apache.hadoop.hive.serde2.objectinspector.primitive.{ 
  PrimitiveObjectInspectorFactory => POIF }
import org.apache.hadoop.hive.serde2.io.ByteWritable
import org.apache.hadoop.hive.serde2.io.DateWritable
import org.apache.hadoop.hive.serde2.io.DoubleWritable
import org.apache.hadoop.hive.serde2.io.ShortWritable
import org.apache.hadoop.hive.serde2.io.TimestampWritable
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector

/**
 * This is the unit test for CG (ROW / OI), the generated source code will be placed under the 
 * folder specified thru jvm flag "cg_source_code_path"
 * 
 * For example:
 * java -cp xxxx -Dcg_source_code_path=/home/hcheng/git/sotc_cloud-shark/cgjava
 */
class TestCGRowOI extends FunSuite with BeforeAndAfterEach {
  implicit def convertArray2JList[T](source: Array[T]): java.util.List[T] = {
    import collection.JavaConversions

    JavaConversions.asJavaList(source)
  }

  // TODO work around solution for sbt unit test
  System.setProperty(JavaCompilerHelper.SBT_UNIT_TEST_WORK_AROUND, "true")

  private var oldCL: ClassLoader = _
  private var cc: CompilationContext = _

  @Before
  override def beforeEach() {
    oldCL = Thread.currentThread().getContextClassLoader()
    cc = new CompilationContext()
    Thread.currentThread().setContextClassLoader(cc.preCompiledClassLoader)
  }

  @After
  override def afterEach() {
    Thread.currentThread().setContextClassLoader(oldCL)
  }

  private def compile(struct: CGStruct, structOI: CGOIStruct) {
    cc.compile(
      List(
        (struct.fullClassName, CGRow.generate(struct, true)),
        (structOI.fullClassName, CGOI.generateOI(structOI, true))
      )
    )
  }

  private def compare(tcgstruct: TCGStruct) {
    val oi: StructObjectInspector = tcgstruct.createOI().asInstanceOf[StructObjectInspector]
    val cgstruct = CGField.create(oi)
    val cgstructoi = CGOIField.create(cgstruct).asInstanceOf[CGOIStruct]

    compile(cgstruct, cgstructoi)
    val row = tcgstruct.feedCG(cgstruct)

    val new_row: AnyRef = KryoSerializer.deserialize(KryoSerializer.serialize(row))

    tcgstruct.compare(new_row)
    tcgstruct.compare(new_row, Class.forName(cgstructoi.fullClassName, true, 
        Thread.currentThread().getContextClassLoader()).newInstance().asInstanceOf[OI])
  }

  test("primitive") {
    val fields = Array[TCGField[AnyRef]](
      new TCGBoolean(),
      new TCGByte(),
      new TCGBinary(),
      new TCGDate(),
      new TCGDouble(),
      new TCGFloat(),
      new TCGInt(),
      new TCGLong(),
      new TCGShort(),
      new TCGString(),
      new TCGTimestamp()
    )

    val tcgstruct = new TCGStruct(EnumConstant.NOT_CONST, "invalid", fields)
    compare(tcgstruct)
  }

  test("constant primitive") {
    val fields = Array[TCGField[AnyRef]](
      new TCGBoolean(EnumConstant.CONST),
      new TCGByte(EnumConstant.CONST),
      new TCGBinary(EnumConstant.CONST),
      new TCGDate(EnumConstant.CONST),
      new TCGDouble(EnumConstant.CONST),
      new TCGFloat(EnumConstant.CONST),
      new TCGInt(EnumConstant.CONST),
      new TCGLong(EnumConstant.CONST),
      new TCGShort(EnumConstant.CONST),
      new TCGString(EnumConstant.CONST),
      new TCGTimestamp(EnumConstant.CONST)
    )

    val tcgstruct = new TCGStruct(EnumConstant.CONST, "invalid", fields)
    compare(tcgstruct)
  }

  test("constant null primitive") {
    val fields = Array[TCGField[AnyRef]](
      new TCGBoolean(EnumConstant.CONST, null),
      new TCGByte(EnumConstant.CONST, null),
      new TCGBinary(EnumConstant.CONST, null),
      new TCGDate(EnumConstant.CONST, null),
      new TCGDouble(EnumConstant.CONST, null),
      new TCGFloat(EnumConstant.CONST, null),
      new TCGInt(EnumConstant.CONST, null),
      new TCGLong(EnumConstant.CONST, null),
      new TCGShort(EnumConstant.CONST, null),
      new TCGString(EnumConstant.CONST, null),
      new TCGTimestamp(EnumConstant.CONST, null)
    )

    val tcgstruct = new TCGStruct(EnumConstant.CONST, "invalid", fields)
    compare(tcgstruct)
  }

  test("struct with map union list struct primitive") {
    val struct = new TCGStruct(EnumConstant.NOT_CONST, "ss", Array[TCGField[AnyRef]](
      new TCGBoolean(),
      new TCGByte(),
      new TCGBinary(),
      new TCGDate(),
      new TCGDouble(),
      new TCGFloat(),
      new TCGInt(),
      new TCGLong(),
      new TCGShort(),
      new TCGString(),
      new TCGTimestamp()
    ))

    val map = new TCGMap(EnumConstant.NOT_CONST, "mm",
      Array[TCGField[AnyRef]](
        new TCGByte(EnumConstant.NOT_CONST, 1.asInstanceOf[Byte], "b1"),
        new TCGByte(EnumConstant.NOT_CONST, 2.asInstanceOf[Byte], "b2")),
      Array[TCGField[AnyRef]](
        new TCGInt(EnumConstant.NOT_CONST, 1.asInstanceOf[Int], "i1"),
        new TCGInt(EnumConstant.NOT_CONST, 2.asInstanceOf[Int], "i2")
      )
    )

    val union = new TCGUnion(EnumConstant.NOT_CONST, "uu", Array[TCGField[AnyRef]](
      new TCGBoolean(),
      new TCGByte(),
      new TCGBinary(),
      new TCGDate(),
      new TCGDouble(),
      new TCGFloat(),
      new TCGInt(),
      new TCGLong(),
      new TCGShort(),
      new TCGString(),
      new TCGTimestamp()
    ), 3)

    val list = new TCGList(EnumConstant.NOT_CONST, "ll", Array[TCGField[AnyRef]](
      new TCGInt(EnumConstant.NOT_CONST, 1.asInstanceOf[Int], "i1"),
      new TCGInt(EnumConstant.NOT_CONST, 2.asInstanceOf[Int], "i2"),
      new TCGInt(EnumConstant.NOT_CONST, 3.asInstanceOf[Int], "i3"),
      new TCGInt(EnumConstant.NOT_CONST, 4.asInstanceOf[Int], "i4"),
      new TCGInt(EnumConstant.NOT_CONST, 5.asInstanceOf[Int], "i5"),
      new TCGInt(EnumConstant.NOT_CONST, 6.asInstanceOf[Int], "i6")
    )
    )

    val tcgstruct = new TCGStruct(EnumConstant.NOT_CONST, "root", Array[TCGField[AnyRef]](
      new TCGBoolean(),
      new TCGByte(),
      new TCGBinary(),
      new TCGDate(),
      new TCGDouble(),
      new TCGFloat(),
      new TCGInt(),
      new TCGLong(),
      new TCGShort(),
      new TCGString(),
      new TCGTimestamp(),
      struct,
      map,
      union,
      list
    )
    )

    compare(tcgstruct)
  }

  //@Test def test() {
  test("struct with map union list struct primitive mixed with const") {
    val struct = new TCGStruct(EnumConstant.NOT_CONST, "ss", Array[TCGField[AnyRef]](
      new TCGBoolean(),
      new TCGByte(EnumConstant.CONST),
      new TCGBinary(EnumConstant.CONST),
      new TCGDate(),
      new TCGDouble(EnumConstant.CONST, null),
      new TCGFloat(),
      new TCGInt(),
      new TCGLong(EnumConstant.CONST),
      new TCGShort(EnumConstant.CONST, null),
      new TCGString(),
      new TCGTimestamp()
    ))

    val map = new TCGMap(EnumConstant.CONST, "mm",
      Array[TCGField[AnyRef]](
        new TCGByte(EnumConstant.CONST, 1.asInstanceOf[Byte], "b1"),
        new TCGByte(EnumConstant.CONST, 2.asInstanceOf[Byte], "b2")),
      Array[TCGField[AnyRef]](
        new TCGInt(EnumConstant.CONST, 1.asInstanceOf[Int], "i1"),
        new TCGInt(EnumConstant.CONST, 2.asInstanceOf[Int], "i2")
      )
    )

    val union1 = new TCGUnion(EnumConstant.NOT_CONST, "u1", Array[TCGField[AnyRef]](
      new TCGBoolean(),
      new TCGByte(),
      new TCGBinary(),
      new TCGDate(EnumConstant.CONST),
      new TCGDouble(),
      new TCGFloat(),
      new TCGInt(),
      new TCGLong(),
      new TCGShort(),
      new TCGString(),
      new TCGTimestamp()
    ), 3)

    val union2 = new TCGUnion(EnumConstant.NOT_CONST, "u2", Array[TCGField[AnyRef]](
      new TCGBoolean(),
      new TCGByte(),
      new TCGBinary(),
      new TCGDate(EnumConstant.CONST, null),
      new TCGDouble(),
      new TCGFloat(),
      new TCGInt(),
      new TCGLong(),
      new TCGShort(),
      new TCGString(),
      new TCGTimestamp()
    ), 4)

    val list = new TCGList(EnumConstant.CONST, "ll", Array[TCGField[AnyRef]](
      new TCGInt(EnumConstant.CONST, 1.asInstanceOf[Int], "i1"),
      new TCGInt(EnumConstant.CONST, null, "i2"),
      new TCGInt(EnumConstant.CONST, 3.asInstanceOf[Int], "i3"),
      new TCGInt(EnumConstant.CONST, null, "i4"),
      new TCGInt(EnumConstant.CONST, 5.asInstanceOf[Int], "i5"),
      new TCGInt(EnumConstant.CONST, 6.asInstanceOf[Int], "i6")
    )
    )

    val tcgstruct = new TCGStruct(EnumConstant.NOT_CONST, "root", Array[TCGField[AnyRef]](
      new TCGBoolean(EnumConstant.CONST),
      new TCGByte(EnumConstant.CONST),
      new TCGBinary(EnumConstant.CONST),
      new TCGDate(EnumConstant.CONST, null),
      new TCGDouble(EnumConstant.CONST),
      new TCGFloat(EnumConstant.CONST),
      new TCGInt(EnumConstant.CONST, null),
      new TCGLong(EnumConstant.CONST),
      new TCGShort(EnumConstant.CONST),
      new TCGString(EnumConstant.CONST),
      new TCGTimestamp(EnumConstant.CONST),
      struct,
      map,
      union1,
      union2,
      list
    )
    )

    compare(tcgstruct)
  }

  test("complicated struct") {
    val struct1 = new TCGStruct(EnumConstant.NOT_CONST, "ss1", Array[TCGField[AnyRef]](
      new TCGBoolean(),
      new TCGByte(EnumConstant.NOT_CONST, 11.asInstanceOf[Byte]),
      new TCGBinary(),
      new TCGDate(),
      new TCGDouble(EnumConstant.CONST, null),
      new TCGFloat(),
      new TCGInt(),
      new TCGLong(),
      new TCGShort(EnumConstant.CONST, null),
      new TCGString(),
      new TCGTimestamp()
    ))

    val struct2 = new TCGStruct(EnumConstant.NOT_CONST, "ss2", Array[TCGField[AnyRef]](
      new TCGBoolean(),
      new TCGByte(EnumConstant.NOT_CONST, 12.asInstanceOf[Byte]),
      new TCGBinary(),
      new TCGDate(),
      new TCGDouble(EnumConstant.CONST, null),
      new TCGFloat(),
      new TCGInt(),
      new TCGLong(),
      new TCGShort(EnumConstant.CONST, null),
      new TCGString(),
      new TCGTimestamp()
    ))

    val union1 = new TCGUnion(EnumConstant.NOT_CONST, "u1", Array[TCGField[AnyRef]](
      new TCGBoolean(),
      new TCGByte(),
      new TCGBinary(),
      new TCGDate(),
      new TCGDouble(),
      new TCGFloat(),
      new TCGInt(),
      new TCGLong(),
      new TCGShort(),
      new TCGString(),
      new TCGTimestamp()
    ), 3.asInstanceOf[Byte])

    val union2 = new TCGUnion(EnumConstant.NOT_CONST, "u2", Array[TCGField[AnyRef]](
      new TCGBoolean(),
      new TCGByte(),
      new TCGBinary(),
      new TCGDate(EnumConstant.CONST, null),
      new TCGDouble(),
      new TCGFloat(),
      new TCGInt(),
      new TCGLong(),
      new TCGShort(),
      new TCGString(),
      new TCGTimestamp()
    ), 4.asInstanceOf[Byte])

    val map1 = new TCGMap(EnumConstant.CONST, "mm1",
      Array[TCGField[AnyRef]](new TCGInt(EnumConstant.NOT_CONST, 12)),
      Array[TCGField[AnyRef]](union1)
    )

    val map2 = new TCGMap(EnumConstant.CONST, "mm2",
      Array[TCGField[AnyRef]](new TCGInt(EnumConstant.NOT_CONST, 12)),
      Array[TCGField[AnyRef]](struct1)
    )

    val list1 = new TCGList(EnumConstant.CONST, "ll1", Array[TCGField[AnyRef]](
      new TCGInt(EnumConstant.NOT_CONST, 1.asInstanceOf[Int], "i1"),
      new TCGInt(EnumConstant.NOT_CONST, 2.asInstanceOf[Int], "i2"),
      new TCGInt(EnumConstant.CONST, null, "i3"),
      new TCGInt(EnumConstant.CONST, null, "i4"),
      new TCGInt(EnumConstant.NOT_CONST, 5.asInstanceOf[Int], "i5"),
      new TCGInt(EnumConstant.NOT_CONST, 6.asInstanceOf[Int], "i6")
    )
    )

    val list2 = new TCGList(EnumConstant.CONST, "ll2", Array[TCGField[AnyRef]](
      new TCGInt(EnumConstant.NOT_CONST, 1.asInstanceOf[Int], "i1"),
      new TCGInt(EnumConstant.NOT_CONST, 2.asInstanceOf[Int], "i2"),
      new TCGInt(EnumConstant.CONST, null, "i3"),
      new TCGInt(EnumConstant.CONST, null, "i4"),
      new TCGInt(EnumConstant.NOT_CONST, 5.asInstanceOf[Int], "i5"),
      new TCGInt(EnumConstant.NOT_CONST, 6.asInstanceOf[Int], "i6")
    )
    )

    val tcgstruct = new TCGStruct(EnumConstant.NOT_CONST, "root", Array[TCGField[AnyRef]](
      new TCGBoolean(EnumConstant.CONST),
      new TCGByte(EnumConstant.CONST),
      new TCGBinary(EnumConstant.CONST),
      new TCGDate(EnumConstant.CONST, null),
      new TCGDouble(EnumConstant.CONST),
      new TCGFloat(EnumConstant.CONST),
      new TCGInt(EnumConstant.CONST, null),
      new TCGLong(EnumConstant.CONST),
      new TCGShort(EnumConstant.CONST),
      new TCGString(EnumConstant.CONST),
      new TCGTimestamp(EnumConstant.CONST),
      map1,
      map2,
      list1,
      list2
    )
    )

    compare(tcgstruct)
  }
  
  test("test the variable name escaping") {
    val struct1 = new TCGStruct(EnumConstant.NOT_CONST, "SStruct1", Array[TCGField[AnyRef]](
      new TCGBoolean(EnumConstant.NOT_CONST, true, "boolean"),
      new TCGByte(EnumConstant.NOT_CONST, 11.asInstanceOf[Byte], "byte"),
      new TCGBinary(EnumConstant.NOT_CONST, Array[Byte](0x1, 0x2, 0x3), "ArrayList"),
      new TCGDate(EnumConstant.NOT_CONST, new Date(1234567890), "Date"),
      new TCGDouble(EnumConstant.CONST, null, "double"),
      new TCGFloat(EnumConstant.NOT_CONST, 4.1f, "float"),
      new TCGInt(EnumConstant.NOT_CONST, 5, "int"),
      new TCGLong(EnumConstant.NOT_CONST, 6l, "long"),
      new TCGShort(EnumConstant.CONST, null, "short"),
      new TCGString(EnumConstant.NOT_CONST, "\"!@#$%^&*()'", "String"),
      new TCGTimestamp(EnumConstant.NOT_CONST, new Timestamp(123456890l), "Timestamp")
    ))

    val struct2 = new TCGStruct(EnumConstant.NOT_CONST, "SStruct2", Array[TCGField[AnyRef]](
      new TCGBoolean(EnumConstant.NOT_CONST, true, "cache"),
      new TCGByte(EnumConstant.NOT_CONST, 12.asInstanceOf[Byte], CGField.STRUCT_MASK_VARIABLE_NAME),
      new TCGBinary(EnumConstant.NOT_CONST, Array[Byte](0x1, 0x2, 0x3), "CGStructField"),
      new TCGDate(EnumConstant.NOT_CONST, new Date(1234567890), "final"),
      new TCGDouble(EnumConstant.CONST, null, "double"),
      new TCGFloat(EnumConstant.NOT_CONST, 4.1f, "transient"),
      new TCGInt(EnumConstant.NOT_CONST, 5, "input"),
      new TCGLong(EnumConstant.NOT_CONST, 6l, "kryo"),
      new TCGShort(EnumConstant.CONST, null, "short"),
      new TCGString(EnumConstant.NOT_CONST, "\"!@#$%^&*()'", "MASK_short"),
      new TCGTimestamp(EnumConstant.NOT_CONST, new Timestamp(123456890l), "Timestamp")
    ))

    val union1 = new TCGUnion(EnumConstant.NOT_CONST, "union", Array[TCGField[AnyRef]](
      new TCGBoolean(),
      new TCGByte(),
      new TCGBinary(),
      new TCGDate(),
      new TCGDouble(),
      new TCGFloat(),
      new TCGInt(),
      new TCGLong(),
      new TCGShort(),
      new TCGString(),
      new TCGTimestamp()
    ), 3.asInstanceOf[Byte])

    val union2 = new TCGUnion(EnumConstant.NOT_CONST, "enum", Array[TCGField[AnyRef]](
      new TCGBoolean(),
      new TCGByte(),
      new TCGBinary(),
      new TCGDate(EnumConstant.CONST, null),
      new TCGDouble(),
      new TCGFloat(),
      new TCGInt(),
      new TCGLong(),
      new TCGShort(),
      new TCGString(),
      new TCGTimestamp()
    ), 4.asInstanceOf[Byte])

    val map1 = new TCGMap(EnumConstant.CONST, "Map",
      Array[TCGField[AnyRef]](new TCGInt(EnumConstant.NOT_CONST, 12)),
      Array[TCGField[AnyRef]](union1)
    )

    val map2 = new TCGMap(EnumConstant.CONST, "HashMap",
      Array[TCGField[AnyRef]](new TCGInt(EnumConstant.NOT_CONST, 12)),
      Array[TCGField[AnyRef]](struct1)
    )

    val list1 = new TCGList(EnumConstant.CONST, "List", Array[TCGField[AnyRef]](
      new TCGInt(EnumConstant.NOT_CONST, 1.asInstanceOf[Int], "hashCode"),
      new TCGInt(EnumConstant.NOT_CONST, 2.asInstanceOf[Int], "equals"),
      new TCGInt(EnumConstant.CONST, null, "i3"),
      new TCGInt(EnumConstant.CONST, null, "i4"),
      new TCGInt(EnumConstant.NOT_CONST, 5.asInstanceOf[Int], "ObjectInspector"),
      new TCGInt(EnumConstant.NOT_CONST, 6.asInstanceOf[Int], "obj")
    )
    )

    val list2 = new TCGList(EnumConstant.CONST, 
      CGField.STRUCT_MASK_VARIABLE_NAME, Array[TCGField[AnyRef]](
      new TCGInt(EnumConstant.NOT_CONST, 1.asInstanceOf[Int], "data"),
      new TCGInt(EnumConstant.NOT_CONST, 2.asInstanceOf[Int], "o"),
      new TCGInt(EnumConstant.CONST, null, "cache"),
      new TCGInt(EnumConstant.CONST, null, "i4"),
      new TCGInt(EnumConstant.NOT_CONST, 5.asInstanceOf[Int], "i5"),
      new TCGInt(EnumConstant.NOT_CONST, 6.asInstanceOf[Int], "i6")
    )
    )

    val tcgstruct = new TCGStruct(EnumConstant.NOT_CONST, "MASK", Array[TCGField[AnyRef]](
      new TCGBoolean(EnumConstant.CONST),
      new TCGByte(EnumConstant.CONST),
      new TCGBinary(EnumConstant.CONST),
      new TCGDate(EnumConstant.CONST, null),
      new TCGDouble(EnumConstant.CONST),
      new TCGFloat(EnumConstant.CONST),
      new TCGInt(EnumConstant.CONST, null),
      new TCGLong(EnumConstant.CONST),
      new TCGShort(EnumConstant.CONST),
      new TCGString(EnumConstant.CONST),
      new TCGTimestamp(EnumConstant.CONST),
      map1,
      map2,
      list1,
      list2
    )
    )

    compare(tcgstruct)
  }
}