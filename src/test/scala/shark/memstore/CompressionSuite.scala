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
import org.apache.hadoop.io.Text
import org.scalatest.FunSuite
import shark.memstore._
import shark.memstore.CompressedTextColumnFormat.DictionaryEncodedColumnFormat


class CompressionSuite extends FunSuite {

  import ColumnSuite.testStringColumn

  def createCompressedBuilder = new Column.TextColumnBuilder(
    new CompressedTextColumnFormat(5, 5), ColumnStats.TextColumnNoStats)

  def createDictionaryEncodedBuilder = new Column.TextColumnBuilder(
    new DictionaryEncodedColumnFormat(5), ColumnStats.TextColumnNoStats)

  def testCompressedIntColumn(data: Array[java.lang.Integer]) = {
    val cf = new CompressedIntColumnFormat(5)
    ColumnSuite.testPrimitiveColumn(
      data,
      new Column.IntColumnBuilder(cf, cf.stats),
      PrimitiveObjectInspectorFactory.javaIntObjectInspector,
      PrimitiveObjectInspectorFactory.writableIntObjectInspector)
  }

  test("CompressedIntColumnFormat") {
    var c: Column = null

    c = testCompressedIntColumn(Array[java.lang.Integer]())

    c = testCompressedIntColumn(Array[java.lang.Integer](null))

    c = testCompressedIntColumn(Array[java.lang.Integer](0))
    assert(c.format.isInstanceOf[CompressedIntColumnFormat.Compressed])
    assert(c.format.asInstanceOf[CompressedIntColumnFormat.Compressed].arr.
      isInstanceOf[CompressedIntColumnFormat.ByteArrayAsIntArray])

    c = testCompressedIntColumn(Array[java.lang.Integer](1, 2, 3, 4, null, -3, null, 32))
    assert(c.format.isInstanceOf[CompressedIntColumnFormat.Compressed])
    assert(c.format.asInstanceOf[CompressedIntColumnFormat.Compressed].arr.
      isInstanceOf[CompressedIntColumnFormat.ByteArrayAsIntArray])

    c = testCompressedIntColumn(Array[java.lang.Integer](65537, 65539))
    assert(c.format.isInstanceOf[CompressedIntColumnFormat.Compressed])
    assert(c.format.asInstanceOf[CompressedIntColumnFormat.Compressed].arr.
      isInstanceOf[CompressedIntColumnFormat.ByteArrayAsIntArray],
      "expected type " + classOf[CompressedIntColumnFormat.ByteArrayAsIntArray] + ", found " +
      c.format.asInstanceOf[CompressedIntColumnFormat.Compressed].arr.getClass)

    c = testCompressedIntColumn(Array[java.lang.Integer](1, 2, 3, 4, null, -3, null, 11111))
    assert(c.format.isInstanceOf[CompressedIntColumnFormat.Compressed])
    assert(c.format.asInstanceOf[CompressedIntColumnFormat.Compressed].arr.
      isInstanceOf[CompressedIntColumnFormat.ShortArrayAsIntArray],
      "expected type " + classOf[CompressedIntColumnFormat.ShortArrayAsIntArray] + ", found " +
      c.format.asInstanceOf[CompressedIntColumnFormat.Compressed].arr.getClass)

    c = testCompressedIntColumn(Array[java.lang.Integer](65537, 65937))
    assert(c.format.isInstanceOf[CompressedIntColumnFormat.Compressed])
    assert(c.format.asInstanceOf[CompressedIntColumnFormat.Compressed].arr.
      isInstanceOf[CompressedIntColumnFormat.ShortArrayAsIntArray],
      "expected type " + classOf[CompressedIntColumnFormat.ShortArrayAsIntArray] + ", found " +
      c.format.asInstanceOf[CompressedIntColumnFormat.Compressed].arr.getClass)

    c = testCompressedIntColumn(Array[java.lang.Integer](1, 2, 3, 4, null, -3, null, 65537))
    assert(c.format.isInstanceOf[CompressedIntColumnFormat.Compressed])
    assert(c.format.asInstanceOf[CompressedIntColumnFormat.Compressed].arr.
      isInstanceOf[CompressedIntColumnFormat.IntArray],
      "expected type " + classOf[CompressedIntColumnFormat.IntArray] + ", found " +
      c.format.asInstanceOf[CompressedIntColumnFormat.Compressed].arr.getClass)
  }

  test("CompressedTextColumnFormat compressed") {
    var c: Column = null

    c = testStringColumn(
      Array[java.lang.String](),
      createCompressedBuilder)

    c = testStringColumn(
      Array[java.lang.String](null),
      createCompressedBuilder)

    c = testStringColumn(
      Array[java.lang.String](""),
      createCompressedBuilder)

    c = testStringColumn(
      Array[java.lang.String]("abcd"),
      createCompressedBuilder)

    val data = Array[String]("0", "1", "2", null, "1")
    c = testStringColumn(
      data,
      createCompressedBuilder)
  }

  test("CompressedTextColumnFormat uncompressed") {
    val c = testStringColumn(
      Array[String]("0", "1", "2", null, "1", "3", "5", "6"),
      createCompressedBuilder).format.asInstanceOf[CompressedTextColumnFormat]
    assert(c.backingColumn.isInstanceOf[UncompressedColumnFormat.TextColumnFormat],
      "expected type " + classOf[UncompressedColumnFormat.TextColumnFormat] +
      ", found " + c.backingColumn.getClass)
  }

  test("DictionaryEncodedColumnFormat") {
    var c: DictionaryEncodedColumnFormat = null

    c = testStringColumn(
      Array[java.lang.String](),
      createDictionaryEncodedBuilder).format.asInstanceOf[DictionaryEncodedColumnFormat]
    assert(c.numDistinctWords == 0)

    c = testStringColumn(
      Array[java.lang.String](null),
      createDictionaryEncodedBuilder).format.asInstanceOf[DictionaryEncodedColumnFormat]
    assert(c.numDistinctWords == 0)

    c = testStringColumn(
      Array[java.lang.String](""),
      createDictionaryEncodedBuilder).format.asInstanceOf[DictionaryEncodedColumnFormat]
    assert(c.numDistinctWords == 1)

    c = testStringColumn(
      Array[java.lang.String]("abcd"),
      createDictionaryEncodedBuilder).format.asInstanceOf[DictionaryEncodedColumnFormat]
    assert(c.numDistinctWords == 1)

    val data = Array[String]("0", "1", "2", null, "1")
    c = testStringColumn(
      data,
      createDictionaryEncodedBuilder).format.asInstanceOf[DictionaryEncodedColumnFormat]
    assert(c.numDistinctWords == data.toSet.size - 1)
  }
}
