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

package shark.memstore

import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory
import org.apache.hadoop.hive.serde2.objectinspector.primitive.{
  AbstractPrimitiveJavaObjectInspector, AbstractPrimitiveWritableObjectInspector,
  BooleanObjectInspector, ByteObjectInspector, ShortObjectInspector, IntObjectInspector,
  LongObjectInspector, FloatObjectInspector, DoubleObjectInspector, StringObjectInspector}
import org.apache.hadoop.io.{BooleanWritable, FloatWritable, IntWritable, LongWritable,
  NullWritable, Text}
import org.apache.hadoop.io.Text
import org.scalatest.FunSuite
import shark.memstore._

class ColumnStatsSuite extends FunSuite {

  import ColumnSuite._

  def testUncompressedPrimitiveColumnWithStats(
    data: Array[_ <: Object],
    javaOi: AbstractPrimitiveJavaObjectInspector,
    writableOi: AbstractPrimitiveWritableObjectInspector): Column = {
    testPrimitiveColumn(
      data,
      ColumnBuilderCreateFunc.uncompressedArrayFormatWithStats(javaOi, 5),
      javaOi,
      writableOi)
  }

  test("ColumnStats no stats") {
    // Make sure they are of the right type. No errors thrown.
    ColumnStats.BooleanColumnNoStats.append(false)
    ColumnStats.ByteColumnNoStats.append(1.toByte)
    ColumnStats.ShortColumnNoStats.append(1.toShort)
    ColumnStats.IntColumnNoStats.append(14324)
    ColumnStats.LongColumnNoStats.append(3452435L)
    ColumnStats.FloatColumnNoStats.append(1.025F)
    ColumnStats.DoubleColumnNoStats.append(1.2143)
    ColumnStats.TextColumnNoStats.append(new Text("abcd"))
    ColumnStats.GenericColumnNoStats.append(new Text("abcd"))
  }

  test("ColumnStats.BooleanColumnStats") {
    var c = new ColumnStats.BooleanColumnStats
    c.append(false)
    assert(c.min == false && c.max == false)
    c.append(false)
    assert(c.min == false && c.max == false)
    c.append(true)
    assert(c.min == false && c.max == true)

    c = new ColumnStats.BooleanColumnStats
    c.append(true)
    assert(c.min == true && c.max == true)
    c.append(false)
    assert(c.min == false && c.max == true)

    val column = testUncompressedPrimitiveColumnWithStats(
      Array[java.lang.Boolean](true, false, null, true, null, true, true, null, null),
      PrimitiveObjectInspectorFactory.javaBooleanObjectInspector,
      PrimitiveObjectInspectorFactory.writableBooleanObjectInspector)
    assert(column.stats.min == false)
    assert(column.stats.max == true)
  }

  test("ColumnStats.ByteColumnStats") {
    var c = new ColumnStats.ByteColumnStats
    c.append(0)
    assert(c.min == 0 && c.max == 0)
    c.append(1)
    assert(c.min == 0 && c.max == 1)
    c.append(-1)
    assert(c.min == -1 && c.max == 1)
    c.append(2)
    assert(c.min == -1 && c.max == 2)
    c.append(-2)
    assert(c.min == -2 && c.max == 2)

    val column = testUncompressedPrimitiveColumnWithStats(
      Array[java.lang.Byte](1, 2, 3, 4, null, -3, null, 32),
      PrimitiveObjectInspectorFactory.javaByteObjectInspector,
      PrimitiveObjectInspectorFactory.writableByteObjectInspector)
    assert(column.stats.min == -3)
    assert(column.stats.max == 32)
  }

  test("ColumnStats.ShortColumnStats") {
    var c = new ColumnStats.ShortColumnStats
    c.append(0)
    assert(c.min == 0 && c.max == 0)
    c.append(1)
    assert(c.min == 0 && c.max == 1)
    c.append(-1)
    assert(c.min == -1 && c.max == 1)
    c.append(1024)
    assert(c.min == -1 && c.max == 1024)
    c.append(-1024)
    assert(c.min == -1024 && c.max == 1024)

    val column = testUncompressedPrimitiveColumnWithStats(
      Array[java.lang.Short](1, 2, 3, 4, null, -3, null, 32),
      PrimitiveObjectInspectorFactory.javaShortObjectInspector,
      PrimitiveObjectInspectorFactory.writableShortObjectInspector)
    assert(column.stats.min == -3)
    assert(column.stats.max == 32)
  }

  test("ColumnStats.IntColumnStats") {
    var c = new ColumnStats.IntColumnStats
    c.append(0)
    assert(c.min == 0 && c.max == 0)
    c.append(1)
    assert(c.min == 0 && c.max == 1)
    c.append(-1)
    assert(c.min == -1 && c.max == 1)
    c.append(65537)
    assert(c.min == -1 && c.max == 65537)
    c.append(-65537)
    assert(c.min == -65537 && c.max == 65537)

    c = new ColumnStats.IntColumnStats
    assert(c.isOrdered && c.isAscending && c.isDescending)
    assert(c.maxDelta == 0)

    c = new ColumnStats.IntColumnStats
    Array(1).foreach(c.append)
    assert(c.isOrdered && c.isAscending && c.isDescending)
    assert(c.maxDelta == 0)

    c = new ColumnStats.IntColumnStats
    Array(1, 2, 3, 3, 4, 22).foreach(c.append)
    assert(c.isOrdered && c.isAscending && !c.isDescending)
    assert(c.maxDelta == 18)

    c = new ColumnStats.IntColumnStats
    Array(22, 1, 0, -5).foreach(c.append)
    assert(c.isOrdered && !c.isAscending && c.isDescending)
    assert(c.maxDelta == 21)

    c = new ColumnStats.IntColumnStats
    Array(22, 1, 24).foreach(c.append)
    assert(!c.isOrdered && !c.isAscending && !c.isDescending)

    val column = testUncompressedPrimitiveColumnWithStats(
      Array[java.lang.Integer](1, 2, 3, 4, null, -3, null, 32),
      PrimitiveObjectInspectorFactory.javaIntObjectInspector,
      PrimitiveObjectInspectorFactory.writableIntObjectInspector)
    assert(column.stats.min == -3)
    assert(column.stats.max == 32)
  }

  test("ColumnStats.LongColumnStats") {
    var c = new ColumnStats.LongColumnStats
    c.append(0)
    assert(c.min == 0 && c.max == 0)
    c.append(1)
    assert(c.min == 0 && c.max == 1)
    c.append(-1)
    assert(c.min == -1 && c.max == 1)
    c.append(Int.MaxValue.toLong + 1L)
    assert(c.min == -1 && c.max == Int.MaxValue.toLong + 1L)
    c.append(Int.MinValue.toLong - 1L)
    assert(c.min == Int.MinValue.toLong - 1L && c.max == Int.MaxValue.toLong + 1L)

    val column = testUncompressedPrimitiveColumnWithStats(
      Array[java.lang.Long](1, 2, 3, 4, null, -3, null, 32),
      PrimitiveObjectInspectorFactory.javaLongObjectInspector,
      PrimitiveObjectInspectorFactory.writableLongObjectInspector)
    assert(column.stats.min == -3)
    assert(column.stats.max == 32)
  }

  test("ColumnStats.FloatColumnStats") {
    var c = new ColumnStats.FloatColumnStats
    c.append(0)
    assert(c.min == 0 && c.max == 0)
    c.append(1)
    assert(c.min == 0 && c.max == 1)
    c.append(-1)
    assert(c.min == -1 && c.max == 1)
    c.append(20.5445F)
    assert(c.min == -1 && c.max == 20.5445F)
    c.append(-20.5445F)
    assert(c.min == -20.5445F && c.max == 20.5445F)

    val column = testUncompressedPrimitiveColumnWithStats(
      Array[java.lang.Float](1.1F, 2.2F, 3.3F, 4.5F, null, -3.2F, null, 32.5F),
      PrimitiveObjectInspectorFactory.javaFloatObjectInspector,
      PrimitiveObjectInspectorFactory.writableFloatObjectInspector)
    assert(column.stats.min == -3.2F)
    assert(column.stats.max == 32.5F)
  }

  test("ColumnStats.DoubleColumnStats") {
    var c = new ColumnStats.DoubleColumnStats
    c.append(0)
    assert(c.min == 0 && c.max == 0)
    c.append(1)
    assert(c.min == 0 && c.max == 1)
    c.append(-1)
    assert(c.min == -1 && c.max == 1)
    c.append(20.5445)
    assert(c.min == -1 && c.max == 20.5445)
    c.append(-20.5445)
    assert(c.min == -20.5445 && c.max == 20.5445)

    val column = testUncompressedPrimitiveColumnWithStats(
      Array[java.lang.Double](1.1, 2.2, 3.3, 4.5, null, -3.2, null, 32.5),
      PrimitiveObjectInspectorFactory.javaDoubleObjectInspector,
      PrimitiveObjectInspectorFactory.writableDoubleObjectInspector)
    assert(column.stats.min == -3.2)
    assert(column.stats.max == 32.5)
  }

  test("ColumnStats.TextColumnStats") {
    implicit def T(str: String): Text = new Text(str)
    var c = new ColumnStats.TextColumnStats
    assert(c.min == null && c.max == null)
    c.append("a")
    assert(c.min.equals(T("a")) && c.max.equals(T("a")))
    c.append("b")
    assert(c.min.equals(T("a")) && c.max.equals(T("b")))
    c.append("b")
    assert(c.min.equals(T("a")) && c.max.equals(T("b")))
    c.append("cccc")
    assert(c.min.equals(T("a")) && c.max.equals(T("cccc")))
    c.append("0987")
    assert(c.min.equals(T("0987")) && c.max.equals(T("cccc")))

    val column = testStringColumn(
      Array[java.lang.String]("abcd", "d", null, "null", null, "e", "ffff"),
      new Column.TextColumnBuilder(
        new UncompressedColumnFormat.TextColumnFormat(5), new ColumnStats.TextColumnStats))
    assert(column.stats.min.equals(new Text("abcd")))
    assert(column.stats.max.equals(new Text("null")))
  }
}
