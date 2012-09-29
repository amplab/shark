package shark

import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory
import org.apache.hadoop.hive.serde2.objectinspector.primitive.{
  AbstractPrimitiveJavaObjectInspector, AbstractPrimitiveWritableObjectInspector,
  BooleanObjectInspector, ByteObjectInspector, ShortObjectInspector, IntObjectInspector,
  LongObjectInspector, FloatObjectInspector, DoubleObjectInspector, StringObjectInspector}
import org.apache.hadoop.io.{BooleanWritable, FloatWritable, IntWritable, LongWritable,
  NullWritable, Text}

import org.scalatest.FunSuite

import shark.memstore._


class ColumnSuite extends FunSuite {

  import ColumnSuite._

  test("BooleanColumn") {
    testPrimitiveColumn(
      Array[java.lang.Boolean](),
      new Column.BooleanColumn.Builder(5),
      PrimitiveObjectInspectorFactory.javaBooleanObjectInspector,
      PrimitiveObjectInspectorFactory.writableBooleanObjectInspector)

    testPrimitiveColumn(
      Array[java.lang.Boolean](null),
      new Column.BooleanColumn.Builder(5),
      PrimitiveObjectInspectorFactory.javaBooleanObjectInspector,
      PrimitiveObjectInspectorFactory.writableBooleanObjectInspector)

    testPrimitiveColumn(
      Array[java.lang.Boolean](false),
      new Column.BooleanColumn.Builder(5),
      PrimitiveObjectInspectorFactory.javaBooleanObjectInspector,
      PrimitiveObjectInspectorFactory.writableBooleanObjectInspector)

    testPrimitiveColumn(
      Array[java.lang.Boolean](true, false, null, true, null, true, true, null, null),
      new Column.BooleanColumn.Builder(5),
      PrimitiveObjectInspectorFactory.javaBooleanObjectInspector,
      PrimitiveObjectInspectorFactory.writableBooleanObjectInspector)
  }

  test("ByteColumn") {
    testPrimitiveColumn(
      Array[java.lang.Byte](),
      new Column.ByteColumn.Builder(5),
      PrimitiveObjectInspectorFactory.javaByteObjectInspector,
      PrimitiveObjectInspectorFactory.writableByteObjectInspector)

    testPrimitiveColumn(
      Array[java.lang.Byte](null),
      new Column.ByteColumn.Builder(5),
      PrimitiveObjectInspectorFactory.javaByteObjectInspector,
      PrimitiveObjectInspectorFactory.writableByteObjectInspector)

    testPrimitiveColumn(
      Array[java.lang.Byte](0),
      new Column.ByteColumn.Builder(5),
      PrimitiveObjectInspectorFactory.javaByteObjectInspector,
      PrimitiveObjectInspectorFactory.writableByteObjectInspector)

    testPrimitiveColumn(
      Array[java.lang.Byte](1, 2, 3, 4, null, -3, null, 32),
      new Column.ByteColumn.Builder(5),
      PrimitiveObjectInspectorFactory.javaByteObjectInspector,
      PrimitiveObjectInspectorFactory.writableByteObjectInspector)
  }

  test("ShortColumn") {
    testPrimitiveColumn(
      Array[java.lang.Short](),
      new Column.ShortColumn.Builder(5),
      PrimitiveObjectInspectorFactory.javaShortObjectInspector,
      PrimitiveObjectInspectorFactory.writableShortObjectInspector)

    testPrimitiveColumn(
      Array[java.lang.Short](null),
      new Column.ShortColumn.Builder(5),
      PrimitiveObjectInspectorFactory.javaShortObjectInspector,
      PrimitiveObjectInspectorFactory.writableShortObjectInspector)

    testPrimitiveColumn(
      Array[java.lang.Short](0),
      new Column.ShortColumn.Builder(5),
      PrimitiveObjectInspectorFactory.javaShortObjectInspector,
      PrimitiveObjectInspectorFactory.writableShortObjectInspector)

    testPrimitiveColumn(
      Array[java.lang.Short](1, 2, 3, 4, null, -3, null, 32),
      new Column.ShortColumn.Builder(5),
      PrimitiveObjectInspectorFactory.javaShortObjectInspector,
      PrimitiveObjectInspectorFactory.writableShortObjectInspector)
  }

  test("IntColumn") {
    testPrimitiveColumn(
      Array[java.lang.Integer](),
      new Column.IntColumn.Builder(5),
      PrimitiveObjectInspectorFactory.javaIntObjectInspector,
      PrimitiveObjectInspectorFactory.writableIntObjectInspector)

    testPrimitiveColumn(
      Array[java.lang.Integer](null),
      new Column.IntColumn.Builder(5),
      PrimitiveObjectInspectorFactory.javaIntObjectInspector,
      PrimitiveObjectInspectorFactory.writableIntObjectInspector)

    testPrimitiveColumn(
      Array[java.lang.Integer](0),
      new Column.IntColumn.Builder(5),
      PrimitiveObjectInspectorFactory.javaIntObjectInspector,
      PrimitiveObjectInspectorFactory.writableIntObjectInspector)

    testPrimitiveColumn(
      Array[java.lang.Integer](1, 2, 3, 4, null, -3, null, 32),
      new Column.IntColumn.Builder(5),
      PrimitiveObjectInspectorFactory.javaIntObjectInspector,
      PrimitiveObjectInspectorFactory.writableIntObjectInspector)
  }

  test("LongColumn") {
    testPrimitiveColumn(
      Array[java.lang.Long](),
      new Column.LongColumn.Builder(5),
      PrimitiveObjectInspectorFactory.javaLongObjectInspector,
      PrimitiveObjectInspectorFactory.writableLongObjectInspector)

    testPrimitiveColumn(
      Array[java.lang.Long](null),
      new Column.LongColumn.Builder(5),
      PrimitiveObjectInspectorFactory.javaLongObjectInspector,
      PrimitiveObjectInspectorFactory.writableLongObjectInspector)

    testPrimitiveColumn(
      Array[java.lang.Long](14),
      new Column.LongColumn.Builder(5),
      PrimitiveObjectInspectorFactory.javaLongObjectInspector,
      PrimitiveObjectInspectorFactory.writableLongObjectInspector)

    testPrimitiveColumn(
      Array[java.lang.Long](1, 2, 3, 4, null, -3, null, 32),
      new Column.LongColumn.Builder(5),
      PrimitiveObjectInspectorFactory.javaLongObjectInspector,
      PrimitiveObjectInspectorFactory.writableLongObjectInspector)
  }

  test("StringColumn") {
    testStringColumn(
      Array[java.lang.String](),
      new Column.StringColumn.Builder(5))

    testStringColumn(
      Array[java.lang.String](null),
      new Column.StringColumn.Builder(5))

    testStringColumn(
      Array[java.lang.String](""),
      new Column.StringColumn.Builder(5))

    testStringColumn(
      Array[java.lang.String]("abcd"),
      new Column.StringColumn.Builder(5))

    testStringColumn(
      Array[java.lang.String]("abcd", "d", null, "null", null, "e", "ffff"),
      new Column.StringColumn.Builder(5))
  }
}


object ColumnSuite {

  implicit def int2Byte(v: Int): java.lang.Byte = v.toByte
  implicit def int2Short(v: Int): java.lang.Short = v.toShort

  def testPrimitiveColumn(
    data: Array[_ <: Object],
    builder: ColumnBuilder,
    javaOi: AbstractPrimitiveJavaObjectInspector,
    writableOi: AbstractPrimitiveWritableObjectInspector): Column = {

    data foreach(builder.add(_, javaOi))
    val column = builder.build
    assert(column.size == data.size)

    var i = 0
    while (i < column.size) {
      //println(data(i) + " " + column(i))
      assert(
        (data(i) == null && column(i) == null) ||
        writableOi.getPrimitiveJavaObject(column(i)) == data(i))
      i += 1
    }

    column
  }

  def testStringColumn(data: Array[String], builder: ColumnBuilder): Column =
    testPrimitiveColumn(
      data,
      builder,
      PrimitiveObjectInspectorFactory.javaStringObjectInspector,
      PrimitiveObjectInspectorFactory.writableStringObjectInspector)

}
