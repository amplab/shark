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

import org.apache.hadoop.hive.serde2.io.{ByteWritable, DoubleWritable, ShortWritable}
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive._
import org.apache.hadoop.io._

import org.scalatest.FunSuite

import shark.memstore.ColumnStats._


class ColumnIteratorSuite extends FunSuite {

  test("non-null boolean column") {
    testNonNullColumnIterator(
      Array[java.lang.Boolean](true, false, true, true, true),
      new BooleanColumnBuilder,
      PrimitiveObjectInspectorFactory.writableBooleanObjectInspector,
      classOf[BooleanColumnIterator])
  }

  test("non-null int column") {
    testNonNullColumnIterator(
      Array[java.lang.Integer](0, 1, 2, 5, 134, -12, 1, 0, 99, 1),
      new IntColumnBuilder,
      PrimitiveObjectInspectorFactory.writableIntObjectInspector,
      classOf[IntColumnIterator])
  }

  def testNonNullColumnIterator[T](
    testData: Array[_ <: Object],
    builder: ColumnBuilder[T],
    writableOi: AbstractPrimitiveWritableObjectInspector,
    iteratorClass: Class[_ <: ColumnIterator]) {

    builder.initialize(5)
    testData.foreach(x => builder.append(x.asInstanceOf[T]))
    val buffer = builder.build

    // parallelize to test concurrency
    (1 to 10).par.foreach { parallelIndex =>
      val iter = iteratorClass.newInstance.asInstanceOf[ColumnIterator]
      iter.initialize(buffer.asReadOnlyBuffer)
      (0 until testData.size).foreach { i =>
        val expected = testData(i)
        val reality = writableOi.getPrimitiveJavaObject(iter.next)
        assert((expected == null && reality == null) || reality == expected,
            "at position " + i + " expected " + expected + ", but saw " + reality)
      }
    }
  }
}

