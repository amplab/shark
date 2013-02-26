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

import java.sql.Timestamp
import java.util.Arrays
import java.util.Properties
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.serde.Constants
import org.apache.hadoop.hive.serde2.SerDe
import org.apache.hadoop.hive.serde2.`lazy`.ByteArrayRef
import org.apache.hadoop.hive.serde2.`lazy`.LazyObjectBase
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinarySerDe
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.ObjectInspectorOptions
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveWritableObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory
import org.apache.hadoop.io.BytesWritable
import org.scalatest.FunSuite
import shark.memstore.ColumnStats.TextColumnNoStats
import org.apache.hadoop.hive.serde2.`lazy`.LazyBinary
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryBinary
import org.apache.hadoop.hive.serde2.`lazy`.LazyFactory
import org.apache.hadoop.hive.serde2.`lazy`.objectinspector.primitive.LazyPrimitiveObjectInspectorFactory


class ColumnSuite extends FunSuite {
  // TODO(rxin): Add unit tests for void and lazy columns.

  import ColumnSuite._

  test("BooleanColumn") {
    testUncompressedPrimitiveColumn(
      Array[java.lang.Boolean](),
      PrimitiveObjectInspectorFactory.javaBooleanObjectInspector,
      PrimitiveObjectInspectorFactory.writableBooleanObjectInspector)

    testUncompressedPrimitiveColumn(
      Array[java.lang.Boolean](null),
      PrimitiveObjectInspectorFactory.javaBooleanObjectInspector,
      PrimitiveObjectInspectorFactory.writableBooleanObjectInspector)

    testUncompressedPrimitiveColumn(
      Array[java.lang.Boolean](false),
      PrimitiveObjectInspectorFactory.javaBooleanObjectInspector,
      PrimitiveObjectInspectorFactory.writableBooleanObjectInspector)

    testUncompressedPrimitiveColumn(
      Array[java.lang.Boolean](true, false, null, true, null, true, true, null, null),
      PrimitiveObjectInspectorFactory.javaBooleanObjectInspector,
      PrimitiveObjectInspectorFactory.writableBooleanObjectInspector)
  }

  test("ByteColumn") {
    testUncompressedPrimitiveColumn(
      Array[java.lang.Byte](),
      PrimitiveObjectInspectorFactory.javaByteObjectInspector,
      PrimitiveObjectInspectorFactory.writableByteObjectInspector)

    testUncompressedPrimitiveColumn(
      Array[java.lang.Byte](null),
      PrimitiveObjectInspectorFactory.javaByteObjectInspector,
      PrimitiveObjectInspectorFactory.writableByteObjectInspector)

    testUncompressedPrimitiveColumn(
      Array[java.lang.Byte](0),
      PrimitiveObjectInspectorFactory.javaByteObjectInspector,
      PrimitiveObjectInspectorFactory.writableByteObjectInspector)

    testUncompressedPrimitiveColumn(
      Array[java.lang.Byte](1, 2, 3, 4, null, -3, null, 32),
      PrimitiveObjectInspectorFactory.javaByteObjectInspector,
      PrimitiveObjectInspectorFactory.writableByteObjectInspector)
  }

  test("ShortColumn") {
    testUncompressedPrimitiveColumn(
      Array[java.lang.Short](),
      PrimitiveObjectInspectorFactory.javaShortObjectInspector,
      PrimitiveObjectInspectorFactory.writableShortObjectInspector)

    testUncompressedPrimitiveColumn(
      Array[java.lang.Short](null),
      PrimitiveObjectInspectorFactory.javaShortObjectInspector,
      PrimitiveObjectInspectorFactory.writableShortObjectInspector)

    testUncompressedPrimitiveColumn(
      Array[java.lang.Short](0),
      PrimitiveObjectInspectorFactory.javaShortObjectInspector,
      PrimitiveObjectInspectorFactory.writableShortObjectInspector)

    testUncompressedPrimitiveColumn(
      Array[java.lang.Short](1, 2, 3, 4, null, -3, null, 32),
      PrimitiveObjectInspectorFactory.javaShortObjectInspector,
      PrimitiveObjectInspectorFactory.writableShortObjectInspector)
  }

  test("IntColumn") {
    testUncompressedPrimitiveColumn(
      Array[java.lang.Integer](),
      PrimitiveObjectInspectorFactory.javaIntObjectInspector,
      PrimitiveObjectInspectorFactory.writableIntObjectInspector)

    testUncompressedPrimitiveColumn(
      Array[java.lang.Integer](null),
      PrimitiveObjectInspectorFactory.javaIntObjectInspector,
      PrimitiveObjectInspectorFactory.writableIntObjectInspector)

    testUncompressedPrimitiveColumn(
      Array[java.lang.Integer](0),
      PrimitiveObjectInspectorFactory.javaIntObjectInspector,
      PrimitiveObjectInspectorFactory.writableIntObjectInspector)

    testUncompressedPrimitiveColumn(
      Array[java.lang.Integer](1, 2, 3, 4, null, -3, null, 32),
      PrimitiveObjectInspectorFactory.javaIntObjectInspector,
      PrimitiveObjectInspectorFactory.writableIntObjectInspector)
  }

  test("LongColumn") {
    testUncompressedPrimitiveColumn(
      Array[java.lang.Long](),
      PrimitiveObjectInspectorFactory.javaLongObjectInspector,
      PrimitiveObjectInspectorFactory.writableLongObjectInspector)

    testUncompressedPrimitiveColumn(
      Array[java.lang.Long](null),
      PrimitiveObjectInspectorFactory.javaLongObjectInspector,
      PrimitiveObjectInspectorFactory.writableLongObjectInspector)

    testUncompressedPrimitiveColumn(
      Array[java.lang.Long](14),
      PrimitiveObjectInspectorFactory.javaLongObjectInspector,
      PrimitiveObjectInspectorFactory.writableLongObjectInspector)

    testUncompressedPrimitiveColumn(
      Array[java.lang.Long](1, 2, 3, 4, null, -3, null, 32),
      PrimitiveObjectInspectorFactory.javaLongObjectInspector,
      PrimitiveObjectInspectorFactory.writableLongObjectInspector)
  }

  test("FloatColumn") {
    testUncompressedPrimitiveColumn(
      Array[java.lang.Float](),
      PrimitiveObjectInspectorFactory.javaFloatObjectInspector,
      PrimitiveObjectInspectorFactory.writableFloatObjectInspector)

    testUncompressedPrimitiveColumn(
      Array[java.lang.Float](null),
      PrimitiveObjectInspectorFactory.javaFloatObjectInspector,
      PrimitiveObjectInspectorFactory.writableFloatObjectInspector)

    testUncompressedPrimitiveColumn(
      Array[java.lang.Float](14.1F),
      PrimitiveObjectInspectorFactory.javaFloatObjectInspector,
      PrimitiveObjectInspectorFactory.writableFloatObjectInspector)

    testUncompressedPrimitiveColumn(
      Array[java.lang.Float](1.1F, 2.2F, 3.3F, 4.5F, null, -3.2F, null, 32.5F),
      PrimitiveObjectInspectorFactory.javaFloatObjectInspector,
      PrimitiveObjectInspectorFactory.writableFloatObjectInspector)
  }

  test("DoubleColumn") {
    testUncompressedPrimitiveColumn(
      Array[java.lang.Double](),
      PrimitiveObjectInspectorFactory.javaDoubleObjectInspector,
      PrimitiveObjectInspectorFactory.writableDoubleObjectInspector)

    testUncompressedPrimitiveColumn(
      Array[java.lang.Double](null),
      PrimitiveObjectInspectorFactory.javaDoubleObjectInspector,
      PrimitiveObjectInspectorFactory.writableDoubleObjectInspector)

    testUncompressedPrimitiveColumn(
      Array[java.lang.Double](14.1),
      PrimitiveObjectInspectorFactory.javaDoubleObjectInspector,
      PrimitiveObjectInspectorFactory.writableDoubleObjectInspector)

    testUncompressedPrimitiveColumn(
      Array[java.lang.Double](1.1, 2.2, 3.3, 4.5, null, -3.2, null, 32.5),
      PrimitiveObjectInspectorFactory.javaDoubleObjectInspector,
      PrimitiveObjectInspectorFactory.writableDoubleObjectInspector)
  }

  test("TextColumn") {
    testStringColumn(Array[java.lang.String](), new Column.TextColumnBuilder(
      new UncompressedColumnFormat.TextColumnFormat(5), TextColumnNoStats))

    testStringColumn(
      Array[java.lang.String](null), new Column.TextColumnBuilder(
      new UncompressedColumnFormat.TextColumnFormat(5), TextColumnNoStats))

    testStringColumn(
      Array[java.lang.String](""), new Column.TextColumnBuilder(
      new UncompressedColumnFormat.TextColumnFormat(5), TextColumnNoStats))

    testStringColumn(
      Array[java.lang.String]("abcd"), new Column.TextColumnBuilder(
      new UncompressedColumnFormat.TextColumnFormat(5), TextColumnNoStats))

    testStringColumn(
      Array[java.lang.String]("abcd", "d", null, "null", null, "e", "ffff"),
      new Column.TextColumnBuilder(
        new UncompressedColumnFormat.TextColumnFormat(5), TextColumnNoStats))
  }

  test("VoidColumn") {
    testUncompressedPrimitiveColumn(
      Array[java.lang.Void](),
      PrimitiveObjectInspectorFactory.javaVoidObjectInspector,
      PrimitiveObjectInspectorFactory.writableVoidObjectInspector)

    testUncompressedPrimitiveColumn(
      Array[java.lang.Void](null.asInstanceOf[java.lang.Void]),
      PrimitiveObjectInspectorFactory.javaVoidObjectInspector,
      PrimitiveObjectInspectorFactory.writableVoidObjectInspector)

    testUncompressedPrimitiveColumn(
      Array[java.lang.Void](null, null),
      PrimitiveObjectInspectorFactory.javaVoidObjectInspector,
      PrimitiveObjectInspectorFactory.writableVoidObjectInspector)
  }

  test("TimestampColumn") {
    testUncompressedPrimitiveColumn(Array[java.sql.Timestamp](),
        PrimitiveObjectInspectorFactory.javaTimestampObjectInspector,
        PrimitiveObjectInspectorFactory.writableTimestampObjectInspector)

    testUncompressedPrimitiveColumn(
        Array[java.sql.Timestamp](
            Timestamp.valueOf("2011-10-02 18:48:05.123"),
            Timestamp.valueOf("2011-11-02 18:48:05.456")),
        PrimitiveObjectInspectorFactory.javaTimestampObjectInspector,
        PrimitiveObjectInspectorFactory.writableTimestampObjectInspector)
  }

  test("BinaryColumn") {
	testUncompressedPrimitiveColumn(Array[LazyBinary](), 
	    LazyPrimitiveObjectInspectorFactory.LAZY_BINARY_OBJECT_INSPECTOR,
	    PrimitiveObjectInspectorFactory.writableBinaryObjectInspector)
	    
	val rowOI = LazyPrimitiveObjectInspectorFactory.LAZY_BINARY_OBJECT_INSPECTOR
    val binary1 = LazyFactory.createLazyPrimitiveClass(rowOI).asInstanceOf[LazyBinary]
    val ref1 = new ByteArrayRef
    val data = Array[Byte](0,1,2)
    ref1.setData(data)
    binary1.init(ref1, 0, 3)
    testUncompressedPrimitiveColumn(Array[LazyBinary](binary1), 
        PrimitiveObjectInspectorFactory.writableBinaryObjectInspector,
        PrimitiveObjectInspectorFactory.writableBinaryObjectInspector, 
        (x, y) => {
          val xdata = x.asInstanceOf[LazyBinary].getWritableObject().getBytes()
        	val ydata = y.asInstanceOf[ByteArrayRef].getData
        	Arrays.equals(xdata, ydata)
        })

  }

}

case class TestClass1(b: Byte, s: Short, i: Int)

case class TestClass2(l: ByteArrayRef, s: String)

object ColumnSuite {

  implicit def int2Byte(v: Int): java.lang.Byte = v.toByte
  implicit def int2Short(v: Int): java.lang.Short = v.toShort

  def initLazyObject(ba: LazyObjectBase, 
      bytes: Array[Byte], start: Int,
      end: Int): Unit = {
    val b = new ByteArrayRef()
    b.setData(bytes)
    ba.init(b, start, end)
  }

  def testUncompressedPrimitiveColumn(
    data: Array[_ <: Object],
    javaOi: AbstractPrimitiveObjectInspector,
    writableOi: AbstractPrimitiveWritableObjectInspector,
    comp: (AnyRef, AnyRef) => Boolean = _.equals(_)): Column = {
    testPrimitiveColumn(
      data,
      ColumnBuilderCreateFunc.uncompressedArrayFormat(javaOi, 5),
      javaOi,
      writableOi,
      comp)
  }

  def testPrimitiveColumn(
    data: Array[_ <: Object],
    builder: Column.ColumnBuilder,
    javaOi: AbstractPrimitiveObjectInspector,
    writableOi: AbstractPrimitiveWritableObjectInspector,
    comp: (AnyRef, AnyRef) => Boolean = _.equals(_)): Column = {

    data foreach(builder.append(_, javaOi))
    val column: Column = builder.build
    assert(column.size == data.size,
      "for dataset: %s\nexpected size: %d, actual size: %d".format(
      data.toSeq, data.size, column.size))

    // Run the validation in parallel to test problems with concurrency.
    (1 to 10).par.foreach { parallelIndex =>
      var i = 0
      var columnIter: ColumnFormatIterator = column.format.iterator
      while (i < column.size) {
        columnIter.nextRow()
        val expected = data(i)

        // null types are constant object inspectors.
        val reality = writableOi match {
          case constOi: ConstantObjectInspector => constOi.getWritableConstantValue()
          case _ => writableOi.getPrimitiveJavaObject(columnIter.current())
        }

        assert((expected == null && reality == null) || comp(expected, reality),
          "at position " + i + " expected " + expected + ", but saw " + reality)
        i += 1
      }
    }

    column
  }

  def testStringColumn(data: Array[String], builder: Column.ColumnBuilder): Column =
    testPrimitiveColumn(
      data,
      builder,
      PrimitiveObjectInspectorFactory.javaStringObjectInspector,
      PrimitiveObjectInspectorFactory.writableStringObjectInspector)

}
