package shark

import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory
import org.apache.hadoop.io.Text
import org.scalatest.FunSuite
import shark.memstore._


class CompressionSuite extends FunSuite {

  import ColumnSuite.testStringColumn
  import ColumnSuite.testPrimitiveColumn

  test("CompressedStringColumn compressed") {
    var c: CompressedStringColumn = null

    c = testStringColumn(
      Array[java.lang.String](),
      new CompressedStringColumn.Builder(5, 10)).asInstanceOf[CompressedStringColumn]

    c = testStringColumn(
      Array[java.lang.String](null),
      new CompressedStringColumn.Builder(5, 10)).asInstanceOf[CompressedStringColumn]

    c = testStringColumn(
      Array[java.lang.String](""),
      new CompressedStringColumn.Builder(5, 10)).asInstanceOf[CompressedStringColumn]

    c = testStringColumn(
      Array[java.lang.String]("abcd"),
      new CompressedStringColumn.Builder(5, 10)).asInstanceOf[CompressedStringColumn]

    val data = Array[String]("0", "1", "2", null, "1")
    c = testStringColumn(
      data, new CompressedStringColumn.Builder(5, 10)).asInstanceOf[CompressedStringColumn]
  }

  test("CompressedStringColumn uncompressed") {
    val c = testPrimitiveColumn(
      Array[String]("0", "1", "2", null, "1", "3"),
      new CompressedStringColumn.Builder(5, 3),
      PrimitiveObjectInspectorFactory.javaStringObjectInspector,
      PrimitiveObjectInspectorFactory.writableStringObjectInspector
    ).asInstanceOf[CompressedStringColumn]
    assert(c.backingColumn.isInstanceOf[Column.StringColumn])
  }

  test("DictionaryEncodedColumn") {

    var c: DictionaryEncodedColumn = null

    c = testStringColumn(
      Array[java.lang.String](),
      new DictionaryEncodedColumn.Builder(5)).asInstanceOf[DictionaryEncodedColumn]
    assert(c.numDistinctWords == 0)

    c = testStringColumn(
      Array[java.lang.String](null),
      new DictionaryEncodedColumn.Builder(5)).asInstanceOf[DictionaryEncodedColumn]
    assert(c.numDistinctWords == 0)

    c = testStringColumn(
      Array[java.lang.String](""),
      new DictionaryEncodedColumn.Builder(5)).asInstanceOf[DictionaryEncodedColumn]
    assert(c.numDistinctWords == 1)

    c = testStringColumn(
      Array[java.lang.String]("abcd"),
      new DictionaryEncodedColumn.Builder(5)).asInstanceOf[DictionaryEncodedColumn]
    assert(c.numDistinctWords == 1)

    val data = Array[String]("0", "1", "2", null, "1")
    c = testStringColumn(
      data, new DictionaryEncodedColumn.Builder(5)).asInstanceOf[DictionaryEncodedColumn]
    assert(c.numDistinctWords == data.toSet.size - 1)
  }
}
