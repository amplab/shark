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

  test("CompressedStringColumn compressed") {
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

  test("CompressedStringColumn uncompressed") {
    val c = testStringColumn(
      Array[String]("0", "1", "2", null, "1", "3"),
      createCompressedBuilder).asInstanceOf[CompressedTextColumnFormat]
    assert(c.backingColumn.isInstanceOf[UncompressedColumnFormat.TextColumnFormat])
  }

  test("DictionaryEncodedColumn") {
    var c: DictionaryEncodedColumnFormat = null

    c = testStringColumn(
      Array[java.lang.String](),
      createDictionaryEncodedBuilder).asInstanceOf[DictionaryEncodedColumnFormat]
    assert(c.numDistinctWords == 0)

    c = testStringColumn(
      Array[java.lang.String](null),
      createDictionaryEncodedBuilder).asInstanceOf[DictionaryEncodedColumnFormat]
    assert(c.numDistinctWords == 0)

    c = testStringColumn(
      Array[java.lang.String](""),
      createDictionaryEncodedBuilder).asInstanceOf[DictionaryEncodedColumnFormat]
    assert(c.numDistinctWords == 1)

    c = testStringColumn(
      Array[java.lang.String]("abcd"),
      createDictionaryEncodedBuilder).asInstanceOf[DictionaryEncodedColumnFormat]
    assert(c.numDistinctWords == 1)

    val data = Array[String]("0", "1", "2", null, "1")
    c = testStringColumn(
      data,
      createDictionaryEncodedBuilder).asInstanceOf[DictionaryEncodedColumnFormat]
    assert(c.numDistinctWords == data.toSet.size - 1)
  }
}
