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

package shark.memstore2

import org.apache.hadoop.hive.serde2.`lazy`.ByteArrayRef
import org.apache.hadoop.hive.serde2.objectinspector.primitive._
import org.apache.hadoop.io._
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector

import org.scalatest.FunSuite

import shark.memstore2.column._
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import shark.memstore2.column.Implicits._
import java.nio.ByteOrder


class ColumnIteratorSuite extends FunSuite {

  val PARALLEL_MODE = true

  test("void column") {
    val builder = new VoidColumnBuilder
    builder.initialize(5, "void")
    builder.append(null, null)
    builder.append(null, null)
    builder.append(null, null)
    val buf = builder.build()

    val iter = ColumnIterator.newIterator(buf)

    iter.next()

    assert(iter.current == null)
    iter.next()
    assert(iter.current == null)
    iter.next()
    assert(iter.current == null)
  }

  test("boolean column") {
    var builder = new BooleanColumnBuilder
    testColumn(
      Array[java.lang.Boolean](true, false, true, true, true),
      builder,
      PrimitiveObjectInspectorFactory.javaBooleanObjectInspector,
      PrimitiveObjectInspectorFactory.writableBooleanObjectInspector,
      classOf[BooleanColumnIterator])
    assert(builder.stats.min === false)
    assert(builder.stats.max === true)

    builder = new BooleanColumnBuilder
    testColumn(
      Array[java.lang.Boolean](null, false, null, true, true),
      builder,
      PrimitiveObjectInspectorFactory.javaBooleanObjectInspector,
      PrimitiveObjectInspectorFactory.writableBooleanObjectInspector,
      classOf[BooleanColumnIterator],
      true)
    assert(builder.stats.min === false)
    assert(builder.stats.max === true)

    builder = new BooleanColumnBuilder
    builder.setCompressionSchemes(new RLE)
    val a = Array.ofDim[java.lang.Boolean](100)
    Range(0,100).foreach { i =>
      a(i) = if (i < 10) true else if (i <80) false else null
    }
    testColumn(
      a,
      builder,
      PrimitiveObjectInspectorFactory.javaBooleanObjectInspector,
      PrimitiveObjectInspectorFactory.writableBooleanObjectInspector,
      classOf[BooleanColumnIterator],
      true)
  }

  test("byte column") {
    var builder = new ByteColumnBuilder
    testColumn(
      Array[java.lang.Byte](1.toByte, 2.toByte, 15.toByte, 55.toByte, 0.toByte, 40.toByte),
      builder,
      PrimitiveObjectInspectorFactory.javaByteObjectInspector,
      PrimitiveObjectInspectorFactory.writableByteObjectInspector,
      classOf[ByteColumnIterator])
    assert(builder.stats.min === 0.toByte)
    assert(builder.stats.max === 55.toByte)

    builder = new ByteColumnBuilder
    testColumn(
      Array[java.lang.Byte](null, 2.toByte, 15.toByte, null, 0.toByte, null),
      builder,
      PrimitiveObjectInspectorFactory.javaByteObjectInspector,
      PrimitiveObjectInspectorFactory.writableByteObjectInspector,
      classOf[ByteColumnIterator],
      true)
    assert(builder.stats.min === 0.toByte)
    assert(builder.stats.max === 15.toByte)

    builder = new ByteColumnBuilder
    builder.setCompressionSchemes(new RLE)
    testColumn(
      Array[java.lang.Byte](null, 2.toByte, 2.toByte, null, 4.toByte, 4.toByte,4.toByte,5.toByte),
      builder,
      PrimitiveObjectInspectorFactory.javaByteObjectInspector,
      PrimitiveObjectInspectorFactory.writableByteObjectInspector,
      classOf[ByteColumnIterator],
      true)
  }

  test("short column") {
    var builder = new ShortColumnBuilder
    testColumn(
      Array[java.lang.Short](1.toShort, 2.toShort, -15.toShort, 355.toShort, 0.toShort, 40.toShort),
      builder,
      PrimitiveObjectInspectorFactory.javaShortObjectInspector,
      PrimitiveObjectInspectorFactory.writableShortObjectInspector,
      classOf[ShortColumnIterator])
    assert(builder.stats.min === -15.toShort)
    assert(builder.stats.max === 355.toShort)

    builder = new ShortColumnBuilder
    testColumn(
      Array[java.lang.Short](1.toShort, 2.toShort, -15.toShort, null, 0.toShort, null),
      builder,
      PrimitiveObjectInspectorFactory.javaShortObjectInspector,
      PrimitiveObjectInspectorFactory.writableShortObjectInspector,
      classOf[ShortColumnIterator],
      true)
    assert(builder.stats.min === -15.toShort)
    assert(builder.stats.max === 2.toShort)

    testColumn(
      Array[java.lang.Short](1.toShort, 2.toShort, 2.toShort, null, 1.toShort, 1.toShort),
      builder,
      PrimitiveObjectInspectorFactory.javaShortObjectInspector,
      PrimitiveObjectInspectorFactory.writableShortObjectInspector,
      classOf[ShortColumnIterator],
      true)
  }

  test("int column") {
    var builder = new IntColumnBuilder
    testColumn(
      Array[java.lang.Integer](0, 1, 2, 5, 134, -12, 1, 0, 99, 1),
      builder,
      PrimitiveObjectInspectorFactory.javaIntObjectInspector,
      PrimitiveObjectInspectorFactory.writableIntObjectInspector,
      classOf[IntColumnIterator])
    assert(builder.stats.min === -12)
    assert(builder.stats.max === 134)

    builder = new IntColumnBuilder
    testColumn(
      Array[java.lang.Integer](null, 1, 2, 5, 134, -12, null, 0, 99, 1),
      builder,
      PrimitiveObjectInspectorFactory.javaIntObjectInspector,
      PrimitiveObjectInspectorFactory.writableIntObjectInspector,
      classOf[IntColumnIterator],
      true)
    assert(builder.stats.min === -12)
    assert(builder.stats.max === 134)

    builder = new IntColumnBuilder
    builder.setCompressionSchemes(new RLE)
    val a = Array.ofDim[java.lang.Integer](100)
    Range(0,100).foreach { i =>
      a(i) = if (i < 10) 10 else if (i <80) 11 else null
    }

    testColumn(
      a,
      builder,
      PrimitiveObjectInspectorFactory.javaIntObjectInspector,
      PrimitiveObjectInspectorFactory.writableIntObjectInspector,
      classOf[IntColumnIterator],
      true)
  }

  test("long column") {
    var builder = new LongColumnBuilder
    testColumn(
      Array[java.lang.Long](1L, -345345L, 15L, 0L, 23445456L),
      builder,
      PrimitiveObjectInspectorFactory.javaLongObjectInspector,
      PrimitiveObjectInspectorFactory.writableLongObjectInspector,
      classOf[LongColumnIterator])
    assert(builder.stats.min === -345345L)
    assert(builder.stats.max === 23445456L)
    builder = new LongColumnBuilder
    testColumn(
      Array[java.lang.Long](null, -345345L, 15L, 0L, null),
      builder,
      PrimitiveObjectInspectorFactory.javaLongObjectInspector,
      PrimitiveObjectInspectorFactory.writableLongObjectInspector,
      classOf[LongColumnIterator],
      true)
    assert(builder.stats.min === -345345L)
    assert(builder.stats.max === 15L)

    builder = new LongColumnBuilder
    builder.setCompressionSchemes(new RLE)
    val a = Array.ofDim[java.lang.Long](100)
    Range(0,100).foreach { i =>
      a(i) = if (i < 10) 10 else if (i <80) 11 else null
    }
    testColumn(
      a,
      builder,
      PrimitiveObjectInspectorFactory.javaLongObjectInspector,
      PrimitiveObjectInspectorFactory.writableLongObjectInspector,
      classOf[LongColumnIterator],
      true)
  }

  test("float column") {
    var builder = new FloatColumnBuilder
    testColumn(
      Array[java.lang.Float](1.1.toFloat, -2.5.toFloat, 20000.toFloat, 0.toFloat, 15.0.toFloat),
      builder,
      PrimitiveObjectInspectorFactory.javaFloatObjectInspector,
      PrimitiveObjectInspectorFactory.writableFloatObjectInspector,
      classOf[FloatColumnIterator])
    assert(builder.stats.min === -2.5.toFloat)
    assert(builder.stats.max === 20000.toFloat)
    builder = new FloatColumnBuilder
    testColumn(
      Array[java.lang.Float](1.1.toFloat, null, 20000.toFloat, null, 15.0.toFloat),
      builder,
      PrimitiveObjectInspectorFactory.javaFloatObjectInspector,
      PrimitiveObjectInspectorFactory.writableFloatObjectInspector,
      classOf[FloatColumnIterator],
      true)
    assert(builder.stats.min === 1.1.toFloat)
    assert(builder.stats.max === 20000.toFloat)
  }

  test("double column") {
    var builder = new DoubleColumnBuilder
    testColumn(
      Array[java.lang.Double](1.1, 2.2, -2.5, 20000, 0, 15.0),
      builder,
      PrimitiveObjectInspectorFactory.javaDoubleObjectInspector,
      PrimitiveObjectInspectorFactory.writableDoubleObjectInspector,
      classOf[DoubleColumnIterator])
    assert(builder.stats.min === -2.5)
    assert(builder.stats.max === 20000)
    builder = new DoubleColumnBuilder
    testColumn(
      Array[java.lang.Double](1.1, 2.2, -2.5, null, 0, 15.0),
      builder,
      PrimitiveObjectInspectorFactory.javaDoubleObjectInspector,
      PrimitiveObjectInspectorFactory.writableDoubleObjectInspector,
      classOf[DoubleColumnIterator],
      true)
    assert(builder.stats.min === -2.5)
    assert(builder.stats.max === 15.0)
  }

  test("string column") {
    var builder = new StringColumnBuilder
    testColumn(
      Array[Text](new Text("a"), new Text(""), new Text("b"), new Text("Abcdz")),
      builder,
      PrimitiveObjectInspectorFactory.writableStringObjectInspector,
      PrimitiveObjectInspectorFactory.writableStringObjectInspector,
      classOf[StringColumnIterator],
      false,
      (a, b) => (a.equals(b.toString))
    )
    assert(builder.stats.min.toString === "")
    assert(builder.stats.max.toString === "b")

    builder = new StringColumnBuilder
    testColumn(
      Array[Text](new Text("a"), new Text(""), null, new Text("b"), new Text("Abcdz"), null),
      builder,
      PrimitiveObjectInspectorFactory.writableStringObjectInspector,
      PrimitiveObjectInspectorFactory.writableStringObjectInspector,
      classOf[StringColumnIterator],
      false,
      (a, b) => if (a == null) b == null else (a.toString.equals(b.toString))
    )
    assert(builder.stats.min.toString === "")
    assert(builder.stats.max.toString === "b")

    builder = new StringColumnBuilder
    builder.setCompressionSchemes(new RLE)
    testColumn(
      Array[Text](new Text("a"), new Text("a"), null, new Text("b"), new Text("b"), new Text("Abcdz")),
      builder,
      PrimitiveObjectInspectorFactory.writableStringObjectInspector,
      PrimitiveObjectInspectorFactory.writableStringObjectInspector,
      classOf[StringColumnIterator],
      false,
      (a, b) => if (a == null) b == null else (a.toString.equals(b.toString))
    )
  }

  test("timestamp column") {
    val ts1 = new java.sql.Timestamp(0)
    val ts2 = new java.sql.Timestamp(500)
    ts2.setNanos(400)
    val ts3 = new java.sql.Timestamp(1362561610000L)

    var builder = new TimestampColumnBuilder
    testColumn(
      Array(ts1, ts2, ts3),
      builder,
      PrimitiveObjectInspectorFactory.javaTimestampObjectInspector,
      PrimitiveObjectInspectorFactory.writableTimestampObjectInspector,
      classOf[TimestampColumnIterator],
      false,
      (a, b) => (a.equals(b))
    )
    assert(builder.stats.min.equals(ts1))
    assert(builder.stats.max.equals(ts3))

    builder = new TimestampColumnBuilder
    testColumn(
      Array(ts1, ts2, null, ts3, null),
      builder,
      PrimitiveObjectInspectorFactory.javaTimestampObjectInspector,
      PrimitiveObjectInspectorFactory.writableTimestampObjectInspector,
      classOf[TimestampColumnIterator],
      true,
      (a, b) => (a.equals(b))
    )
    assert(builder.stats.min.equals(ts1))
    assert(builder.stats.max.equals(ts3))
  }

  test("Binary Column") {
    val b1 = new BytesWritable()
    b1.set(Array[Byte](0,1,2), 0, 3)

    val builder = new BinaryColumnBuilder
    testColumn(
      Array[BytesWritable](b1),
      builder,
      PrimitiveObjectInspectorFactory.writableBinaryObjectInspector,
      PrimitiveObjectInspectorFactory.writableBinaryObjectInspector,
      classOf[BinaryColumnIterator],
      false,
      compareBinary)
    assert(builder.stats.isInstanceOf[ColumnStats.NoOpStats[_]])
 
    def compareBinary(x: Object, y: Object): Boolean = {
      val xdata = x.asInstanceOf[Array[Byte]]
      val ywritable = y.asInstanceOf[BytesWritable]
      val ydata = ywritable.getBytes()
      val length = ywritable.getLength()
      if (length != xdata.length) {
        false
      } else {
        val ydatapruned = new Array[Byte](length)
        System.arraycopy(ydata, 0, ydatapruned, 0, length)
        java.util.Arrays.equals(xdata, ydatapruned)
      }
    }
  }


  def testColumn[T, U <: ColumnIterator](
    testData: Array[_ <: Object],
    builder: ColumnBuilder[T],
    oi: ObjectInspector,
    writableOi: AbstractPrimitiveWritableObjectInspector,
    iteratorClass: Class[U],
    expectEWAHWrapper: Boolean = false,
    compareFunc: (Object, Object) => Boolean = (a, b) => a == b) {

    builder.initialize(testData.size, "")
    testData.foreach { x => builder.append(x, oi)}
    val buf = builder.build()

    def executeOneTest() {
      val iter = ColumnIterator.newIterator(buf)

      (0 until testData.size).foreach { i =>
        iter.next()
        val expected = testData(i)
        val reality = writableOi.getPrimitiveJavaObject(iter.current)
        //println ("at position " + i + " expected " + expected + ", but saw " + reality)
        assert((expected == null && reality == null) || compareFunc(reality, expected),
           "at position " + i + " expected " + expected + ", but saw " + reality)
      }
    }

    if (PARALLEL_MODE) {
      // parallelize to test concurrency
      (1 to 10).par.foreach { parallelIndex => executeOneTest() }
    } else {
      executeOneTest()
    }
  }
}

