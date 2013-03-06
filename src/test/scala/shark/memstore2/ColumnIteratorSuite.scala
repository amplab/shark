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

import java.sql.Timestamp

import org.apache.hadoop.hive.serde2.io.ByteWritable
import org.apache.hadoop.hive.serde2.io.DoubleWritable
import org.apache.hadoop.hive.serde2.io.ShortWritable
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive._
import org.apache.hadoop.io._

import org.scalatest.FunSuite

import shark.memstore2.column._


class ColumnIteratorSuite extends FunSuite {

  val PARALLEL_MODE = false

  test("non-null void column") {
    val builder = new VoidColumnBuilder
    builder.initialize(5)
    builder.append(null, null)
    builder.appendNull()
    builder.append(null, null)
    val buffer = builder.build

    val iter = new VoidColumnIterator
    iter.initialize(buffer)
    assert(iter.next == NullWritable.get())
    assert(iter.next == NullWritable.get())
    assert(iter.next == NullWritable.get())
  }

  test("non-null boolean column") {
    testNonNullColumnIterator(
      Array[java.lang.Boolean](true, false, true, true, true),
      new BooleanColumnBuilder,
      PrimitiveObjectInspectorFactory.writableBooleanObjectInspector,
      classOf[BooleanColumnIterator])
  }

  test("non-null byte column") {
    testNonNullColumnIterator(
      Array[java.lang.Byte](1.toByte, 2.toByte, 15.toByte, 55.toByte, 0.toByte, 40.toByte),
      new ByteColumnBuilder,
      PrimitiveObjectInspectorFactory.writableByteObjectInspector,
      classOf[ByteColumnIterator])
  }

  test("non-null short column") {
    testNonNullColumnIterator(
      Array[java.lang.Short](1.toShort, 2.toShort, -15.toShort, 355.toShort, 0.toShort, 40.toShort),
      new ShortColumnBuilder,
      PrimitiveObjectInspectorFactory.writableShortObjectInspector,
      classOf[ShortColumnIterator])
  }

  test("non-null int column") {
    testNonNullColumnIterator(
      Array[java.lang.Integer](0, 1, 2, 5, 134, -12, 1, 0, 99, 1),
      new IntColumnBuilder,
      PrimitiveObjectInspectorFactory.writableIntObjectInspector,
      classOf[IntColumnIterator])
  }

  test("non-null long column") {
    testNonNullColumnIterator(
      Array[java.lang.Long](1.toShort, -345345.toShort, 15.toShort, 0.toShort, 23445456.toShort),
      new LongColumnBuilder,
      PrimitiveObjectInspectorFactory.writableLongObjectInspector,
      classOf[LongColumnIterator])
  }

  test("non-null float column") {
    testNonNullColumnIterator(
      Array[java.lang.Float](1.1.toFloat, -2.5.toFloat, 20000.toFloat, 0.toFloat, 15.0.toFloat),
      new FloatColumnBuilder,
      PrimitiveObjectInspectorFactory.writableFloatObjectInspector,
      classOf[FloatColumnIterator])
  }

  test("non-null double column") {
    testNonNullColumnIterator(
      Array[java.lang.Double](1.1, 2.2, -2.5, 20000, 0, 15.0),
      new DoubleColumnBuilder,
      PrimitiveObjectInspectorFactory.writableDoubleObjectInspector,
      classOf[DoubleColumnIterator])
  }

  test("non-null string column") {
    testNonNullColumnIterator(
      Array[Text](new Text("a"), new Text(""), new Text("b"), new Text("Abcdz")),
      new StringColumnBuilder,
      PrimitiveObjectInspectorFactory.writableStringObjectInspector,
      classOf[StringColumnIterator],
      (a, b) => (a.equals(b.toString))
    )
  }

  test("non-null timestamp column") {
    val tsnano = new java.sql.Timestamp(500)
    tsnano.setNanos(400)
    val data = Array(new java.sql.Timestamp(0), tsnano, new java.sql.Timestamp(1362561610000L))

    testNonNullColumnIterator(
      data,
      new TimestampColumnBuilder,
      PrimitiveObjectInspectorFactory.writableTimestampObjectInspector,
      classOf[TimestampColumnIterator],
      (a, b) => (a.equals(b))
    )
  }

  def testNonNullColumnIterator[T](
    testData: Array[_ <: Object],
    builder: ColumnBuilder[T],
    writableOi: AbstractPrimitiveWritableObjectInspector,
    iteratorClass: Class[_ <: ColumnIterator],
    compareFunc: (Object, Object) => Boolean = (a, b) => a == b) {

    builder.initialize(5)
    testData.foreach(x => builder.append(x.asInstanceOf[T]))
    val buffer = builder.build

    def executeOneTest() {
      val iter = iteratorClass.newInstance.asInstanceOf[ColumnIterator]
      iter.initialize(buffer)
      (0 until testData.size).foreach { i =>
        val expected = testData(i)
        val reality = writableOi.getPrimitiveJavaObject(iter.next)
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

