package shark.memstore

import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory
import org.apache.hadoop.hive.serde2.objectinspector.primitive.{
  AbstractPrimitiveJavaObjectInspector, AbstractPrimitiveWritableObjectInspector,
  BooleanObjectInspector, ByteObjectInspector, ShortObjectInspector, IntObjectInspector,
  LongObjectInspector, FloatObjectInspector, DoubleObjectInspector, StringObjectInspector}
import org.apache.hadoop.io.{BooleanWritable, FloatWritable, IntWritable, LongWritable,
  NullWritable, Text}

import org.scalatest.FunSuite

import shark.memstore.ColumnStats._


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
}

object ColumnSuite {

  implicit def int2Byte(v: Int): java.lang.Byte = v.toByte
  implicit def int2Short(v: Int): java.lang.Short = v.toShort

  def testUncompressedPrimitiveColumn(
    data: Array[_ <: Object],
    javaOi: AbstractPrimitiveJavaObjectInspector,
    writableOi: AbstractPrimitiveWritableObjectInspector): Column = {
    testPrimitiveColumn(
      data,
      ColumnBuilderCreateFunc.uncompressedArrayFormat(javaOi, 5),
      javaOi,
      writableOi)
  }

  def testPrimitiveColumn(
    data: Array[_ <: Object],
    builder: Column.ColumnBuilder,
    javaOi: AbstractPrimitiveJavaObjectInspector,
    writableOi: AbstractPrimitiveWritableObjectInspector): Column = {

    data foreach(builder.append(_, javaOi))
    val column: Column = builder.build
    assert(column.size == data.size)

    var i = 0
    var columnIter: ColumnFormatIterator = column.format.iterator
    while (i < column.size) {
      //println(data(i) + " " + column(i))
      columnIter.nextRow()
      val expected = data(i)
      val reality = writableOi.getPrimitiveJavaObject(columnIter.current())
      assert((expected == null && reality == null) || reality == expected,
        "at position " + i + " expected " + expected + ", but saw " + reality)
      i += 1
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
