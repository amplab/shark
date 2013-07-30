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

import it.unimi.dsi.fastutil.ints.IntArrayList
import java.sql.Timestamp
import java.nio.ByteBuffer
import java.nio.ByteOrder

import org.apache.hadoop.hive.serde2.io.ByteWritable
import org.apache.hadoop.hive.serde2.io.DoubleWritable
import org.apache.hadoop.hive.serde2.io.ShortWritable
import org.apache.hadoop.hive.serde2.`lazy`.ByteArrayRef
import org.apache.hadoop.hive.serde2.`lazy`.LazyBinary
import org.apache.hadoop.hive.serde2.`lazy`.LazyFactory
import org.apache.hadoop.hive.serde2.`lazy`.objectinspector.primitive.LazyPrimitiveObjectInspectorFactory
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryBinary
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive._
import org.apache.hadoop.io._

import org.scalatest.FunSuite
import collection.mutable.{Set, HashSet, ListBuffer}

import shark.memstore2.buffer.ByteBufferReader
import shark.memstore2.column._


class ColumnIteratorSuite extends FunSuite {

  val PARALLEL_MODE = true

  test("void column") {
    val builder = new VoidColumnBuilder
    builder.initialize(5)
    builder.append(null, null)
    builder.appendNull()
    builder.append(null, null)
    val buf = builder.build

    val bufreader = ByteBufferReader.createUnsafeReader(buf)
    val columnType = bufreader.getLong().toInt

    val factory = ColumnIterator.getFactory(columnType)
    assert(factory.createIterator(bufreader).getClass === classOf[VoidColumnIterator.Default])


    val iter = new VoidColumnIterator.Default(ByteBufferReader.createUnsafeReader(buf))
    iter.next()
    assert(iter.current == NullWritable.get())
    iter.next()
    assert(iter.current == NullWritable.get())
    iter.next()
    assert(iter.current == NullWritable.get())
  }

  test("boolean column") {
    var builder = new BooleanColumnBuilder
    testColumn(
      Array[java.lang.Boolean](true, false, true, true, true),
      builder,
      PrimitiveObjectInspectorFactory.writableBooleanObjectInspector,
      classOf[BooleanColumnIterator.Default])
    assert(builder.stats.min === false)
    assert(builder.stats.max === true)

    testColumn(
      Array[java.lang.Boolean](null, false, null, true, true),
      builder,
      PrimitiveObjectInspectorFactory.writableBooleanObjectInspector,
      classOf[BooleanColumnIterator.Default],
      true)
    assert(builder.stats.min === false)
    assert(builder.stats.max === true)
  }

  test("byte column") {
    val builder = new ByteColumnBuilder
    testColumn(
      Array[java.lang.Byte](1.toByte, 2.toByte, 15.toByte, 55.toByte, 0.toByte, 40.toByte),
      builder,
      PrimitiveObjectInspectorFactory.writableByteObjectInspector,
      classOf[ByteColumnIterator.Default])
    assert(builder.stats.min === 0.toByte)
    assert(builder.stats.max === 55.toByte)

    testColumn(
      Array[java.lang.Byte](null, 2.toByte, 15.toByte, null, 0.toByte, null),
      builder,
      PrimitiveObjectInspectorFactory.writableByteObjectInspector,
      classOf[ByteColumnIterator.Default],
      true)
    assert(builder.stats.min === 0.toByte)
    assert(builder.stats.max === 15.toByte)
  }

  test("short column") {
    val builder = new ShortColumnBuilder
    testColumn(
      Array[java.lang.Short](1.toShort, 2.toShort, -15.toShort, 355.toShort, 0.toShort, 40.toShort),
      builder,
      PrimitiveObjectInspectorFactory.writableShortObjectInspector,
      classOf[ShortColumnIterator.Default])
    assert(builder.stats.min === -15.toShort)
    assert(builder.stats.max === 355.toShort)

    testColumn(
      Array[java.lang.Short](1.toShort, 2.toShort, -15.toShort, null, 0.toShort, null),
      builder,
      PrimitiveObjectInspectorFactory.writableShortObjectInspector,
      classOf[ShortColumnIterator.Default],
      true)
    assert(builder.stats.min === -15.toShort)
    assert(builder.stats.max === 2.toShort)
  }

  test("dictionary encoding Int") {
    val l = List[Int](1,22,30,4)
    val d = new IntDictionary(l.toArray)

    assert(d.get(0) == 1)

    val buf = ByteBuffer.allocate(2048)
    buf.order(ByteOrder.nativeOrder())
    buf.putInt(5);
    buf.rewind
    assert(5 == buf.getInt())
    buf.rewind

    IntDictionarySerializer.writeToBuffer(buf, d)
    buf.rewind

    val bbr = ByteBufferReader.createUnsafeReader(buf)

    val newd = IntDictionarySerializer.readFromBuffer(bbr)

    assert(d.get(0) == newd.get(0))
    assert(d.get(3) == newd.get(3))

    val seqNull  = ( List(),              classOf[DictionaryEncodedIntColumnIterator.Default] )
    val seqSmall = ( Range(-10, 10, 1),   classOf[DictionaryEncodedIntColumnIterator.Default] )
    val seq254   = ( Range(-127, 127, 1), classOf[DictionaryEncodedIntColumnIterator.Default] )
    val seq255   = ( Range(-127, 128, 1), classOf[DictionaryEncodedIntColumnIterator.Default] )
    val seq256   = ( Range(-127, 129, 1), classOf[IntColumnIterator.Default] )
    val seqBig   = ( Range(-127, 256, 1), classOf[IntColumnIterator.Default] )
    assert(seq256._1.size === 256)

    List(seqNull, seqSmall, seq254, seq255, seq256, seqBig).foreach { s =>
      val seqJava : Seq[java.lang.Integer] = for {
        i <- s._1
      } yield new java.lang.Integer(i)

      val builder = new IntColumnBuilder
      testColumn(
        seqJava,
        builder,
        PrimitiveObjectInspectorFactory.writableIntObjectInspector,
        s._2,
        false)
    }

    intercept[IllegalArgumentException] {
      val tooBig = new IntDictionary(seqBig._1.toArray)
    }
  }
 
  test("LZF Int") {
    var builder = new IntColumnBuilder
    builder.scheme = CompressionScheme.LZF

    testColumn(
      Array[java.lang.Integer](1, 2, 5, 134, -12, 0, 99, 1),
      builder,
      PrimitiveObjectInspectorFactory.writableIntObjectInspector,
      classOf[LZFBlockColumnIterator[IntColumnIterator.Default]],
      false)
    assert(builder.stats.min === -12)
    assert(builder.stats.max === 134)

    builder = new IntColumnBuilder
    testColumn(
      Array[java.lang.Integer]
        (null, 1, 2, null, 5, 134, -12, null, 0, 99, null, null, null, 1),
      builder,
      PrimitiveObjectInspectorFactory.writableIntObjectInspector,
      classOf[LZFBlockColumnIterator[IntColumnIterator.Default]],
      true)
    assert(builder.stats.min === -12)
    assert(builder.stats.max === 134)

  }

  test("RLE") {

    def testRLE(l: List[Int]) = {
      // test batch encode
      var rle = RLESerializer.encode(l)
      var rle_decoded = RLESerializer.decode(rle)
      assert(rle_decoded === l)
      // test one-at-a-time encode
      var rleSs = new RLEStreamingSerializer[Int]( { () => -1 } )
      l.foreach(rleSs.encodeSingle(_))
      rle_decoded = RLESerializer.decode(rleSs.getCoded)
      assert(rle_decoded === l)
    }

    def convertList(l: List[Int]): IntArrayList = {
      var ret = new IntArrayList(l.size)
      l.foreach { x => ret.add(x) }
      ret
    }

    // no runs
    testRLE(List[Int](1,22,30,4))
    // same repeating value
    testRLE(List[Int](5,5,5))
    // with runs
    val l = List[Int](1,22,22,30,4,5,5,5)
    val runs = List[Int](1,2,1,1,3)
    val values = List[Int](1,22,30,4,5)
    testRLE(l)

    // does serializing runs alone work?
    var buf = ByteBuffer.allocate(2048)
    buf.order(ByteOrder.nativeOrder())

    RLESerializer.writeToBuffer(buf, convertList(l))
    buf.rewind

    var bbr = ByteBufferReader.createUnsafeReader(buf)
    val (numNewl, newl) = RLESerializer.readFromBuffer(bbr)
    var lb = new ListBuffer[Int]()
    var j = 0
    while (j < numNewl) {
      lb  += newl.getInt()
      j += 1
    }
    assert(lb.toList == l)

    // does layered serialization of runs+values & iterator work?
    buf.rewind
    RLESerializer.writeToBuffer(buf, convertList(runs))
    values.foreach { i =>
      buf.putInt(i)
    }
    buf.rewind

    bbr = ByteBufferReader.createUnsafeReader(buf)
    var it = new RLEColumnIterator(classOf[IntColumnIterator.Default], bbr)
    var i = 0
    while(i < l.size) {
      it.next
      val writableOi = PrimitiveObjectInspectorFactory.writableIntObjectInspector
      assert(l(i) === writableOi.getPrimitiveJavaObject(it.current))
      i += 1
    }

    {
      assert(true  === StringColumnBuilder.textEquals(new Text("a"), new Text("a")))
      assert(false === StringColumnBuilder.textEquals(new Text("a"), new Text("b")))
      assert(false === StringColumnBuilder.textEquals(new Text("a"), null))
      assert(false === StringColumnBuilder.textEquals(new Text(""),  null))
      assert(true  === StringColumnBuilder.textEquals(new Text(""),  new Text("")))
      assert(true  === StringColumnBuilder.textEquals(null,          null))

      // and now for strings
      var l = List[Text](new Text("a"), new Text("b"), new Text("b"), new Text("Abc"))
      var runs = List[Int](1,2,1)
      var values = List[Text](new Text("a"), new Text("b"), new Text("Abc"))

      // non-streaming
      {
        val rle = RLESerializer.encode(l)
        val rle_decoded = RLESerializer.decode(rle)
        assert(rle_decoded === l)
      }

      // streaming
      {
        var rle = new RLEStreamingSerializer[Text]({ () => null }, StringColumnBuilder.textEquals)
        l.foreach { x => rle.encodeSingle(x) }
        val rle_decoded = RLESerializer.decode(rle.getCoded)
        assert(rle_decoded === l)
      }

      // does serializing work?
      var buf = ByteBuffer.allocate(204800)
      buf.order(ByteOrder.nativeOrder())

      RLESerializer.writeToBuffer(buf, convertList(runs))
      buf.rewind

      var bbr = ByteBufferReader.createUnsafeReader(buf)
      val (newrunsSize, newruns) = RLESerializer.readFromBuffer(bbr)

      var lb = new ListBuffer[Int]()
      var j = 0
      while (j < newrunsSize) {
        lb  += newruns.getInt()
        j += 1
      }
      assert(lb.toList == runs)
    }

  }

  test("int column") {
    var builder = new IntColumnBuilder
    testColumn(
      Array[java.lang.Integer](1, 2, 5, 134, -12, 0, 99, 1),
      builder,
      PrimitiveObjectInspectorFactory.writableIntObjectInspector,
      classOf[DictionaryEncodedIntColumnIterator.Default],
      false)
    assert(builder.stats.min === -12)
    assert(builder.stats.max === 134)

    builder = new IntColumnBuilder
    testColumn(
      Array[java.lang.Integer]
        (null, 1, 2, null, 5, 134, -12, null, 0, 99, null, null, null, 1),
      builder,
      PrimitiveObjectInspectorFactory.writableIntObjectInspector,
      classOf[DictionaryEncodedIntColumnIterator.Default],
      true)
    assert(builder.stats.min === -12)
    assert(builder.stats.max === 134)

    val repeats = List.fill(20000)(2)
    val seqWithRepeats = List.concat(repeats, Range(-100, 100, 1))
    val seqWRJava : Seq[java.lang.Integer] = for {
      i <- seqWithRepeats
    } yield new java.lang.Integer(i)

    builder = new IntColumnBuilder
    testColumn(
      seqWRJava,
      builder,
      PrimitiveObjectInspectorFactory.writableIntObjectInspector,
      classOf[RLEColumnIterator[IntColumnIterator.Default]],
      false)
    assert(builder.stats.min === -100)
    assert(builder.stats.max === 99)

    // too many unique values (>256) - dict compression should not turn on
    val list = Range(-300, 300, 1)
    val seqJava : Seq[java.lang.Integer] = for {
      i <- list
    } yield new java.lang.Integer(i)

    builder = new IntColumnBuilder
    testColumn(
      seqJava,
      builder,
      PrimitiveObjectInspectorFactory.writableIntObjectInspector,
      classOf[IntColumnIterator.Default],
      false)
    assert(builder.stats.min === -300)
    assert(builder.stats.max === 299)


    val nulls = List.fill(10000)(null)
    val seqWithNull = List.concat(nulls, seqJava)
    assert(seqWithNull.size === 10600)

    builder = new IntColumnBuilder
    testColumn(
      seqWithNull,
      builder,
      PrimitiveObjectInspectorFactory.writableIntObjectInspector,
      classOf[IntColumnIterator.Default],
      true)
    assert(builder.stats.min === -300)
    assert(builder.stats.max === 299)
  }

  test("long column") {
    var builder = new LongColumnBuilder
    testColumn(
      Array[java.lang.Long](1L, -345345L, 15L, 0L, 23445456L),
      builder,
      PrimitiveObjectInspectorFactory.writableLongObjectInspector,
      classOf[LongColumnIterator.Default])
    assert(builder.stats.min === -345345L)
    assert(builder.stats.max === 23445456L)

    builder = new LongColumnBuilder
    testColumn(
      Array[java.lang.Long](null, -345345L, 15L, 0L, null),
      builder,
      PrimitiveObjectInspectorFactory.writableLongObjectInspector,
      classOf[LongColumnIterator.Default],
      true)
    assert(builder.stats.min === -345345L)
    assert(builder.stats.max === 15L)
  }

  test("float column") {
    val builder = new FloatColumnBuilder
    testColumn(
      Array[java.lang.Float](1.1.toFloat, -2.5.toFloat, 20000.toFloat, 0.toFloat, 15.0.toFloat),
      builder,
      PrimitiveObjectInspectorFactory.writableFloatObjectInspector,
      classOf[FloatColumnIterator.Default])
    assert(builder.stats.min === -2.5.toFloat)
    assert(builder.stats.max === 20000.toFloat)

    testColumn(
      Array[java.lang.Float](1.1.toFloat, null, 20000.toFloat, null, 15.0.toFloat),
      builder,
      PrimitiveObjectInspectorFactory.writableFloatObjectInspector,
      classOf[FloatColumnIterator.Default],
      true)
    assert(builder.stats.min === 1.1.toFloat)
    assert(builder.stats.max === 20000.toFloat)
  }

  test("double column") {
    val builder = new DoubleColumnBuilder
    testColumn(
      Array[java.lang.Double](1.1, 2.2, -2.5, 20000, 0, 15.0),
      builder,
      PrimitiveObjectInspectorFactory.writableDoubleObjectInspector,
      classOf[DoubleColumnIterator.Default])
    assert(builder.stats.min === -2.5)
    assert(builder.stats.max === 20000)

    testColumn(
      Array[java.lang.Double](1.1, 2.2, -2.5, null, 0, 15.0),
      builder,
      PrimitiveObjectInspectorFactory.writableDoubleObjectInspector,
      classOf[DoubleColumnIterator.Default],
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
      classOf[StringColumnIterator.Default],
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
      classOf[StringColumnIterator.Default],
      false,
      (a, b) => (a.equals(b.toString))
    )
    assert(builder.stats.min.toString === "")
    assert(builder.stats.max.toString === "b")

    val repeats = List.fill(200)(new Text("zz"))
    val seqWithRepeats = List.concat(repeats,
      Array[Text](new Text("a"), new Text(""), null, null, null, new Text("b"), 
        new Text("Abcdz")))
    val seqWithRepeatedRepeats = List.concat(seqWithRepeats, seqWithRepeats, seqWithRepeats, seqWithRepeats)
    assert(seqWithRepeatedRepeats.size === 207*4)

    builder = new StringColumnBuilder
    testColumn(
      seqWithRepeatedRepeats,
      builder,
      PrimitiveObjectInspectorFactory.writableStringObjectInspector,
      classOf[RLEColumnIterator[StringColumnIterator.Default]],
      false,
      (a, b) => (a.equals(b.toString))
    )

    builder = new StringColumnBuilder
    builder.scheme = CompressionScheme.LZF
    testColumn(
      seqWithRepeatedRepeats,
      builder,
      PrimitiveObjectInspectorFactory.writableStringObjectInspector,
      classOf[LZFBlockColumnIterator[StringColumnIterator.Default]],
      false,
      (a, b) => (a.equals(b.toString))
    )

  }

  test("timestamp column") {
    val ts1 = new java.sql.Timestamp(0)
    val ts2 = new java.sql.Timestamp(500)
    ts2.setNanos(400)
    val ts3 = new java.sql.Timestamp(1362561610000L)

    val builder = new TimestampColumnBuilder
    testColumn(
      Array(ts1, ts2, ts3),
      builder,
      PrimitiveObjectInspectorFactory.writableTimestampObjectInspector,
      classOf[TimestampColumnIterator.Default],
      false,
      (a, b) => (a.equals(b))
    )
    assert(builder.stats.min.equals(ts1))
    assert(builder.stats.max.equals(ts3))

    testColumn(
      Array(ts1, ts2, null, ts3, null),
      builder,
      PrimitiveObjectInspectorFactory.writableTimestampObjectInspector,
      classOf[TimestampColumnIterator.Default],
      true,
      (a, b) => (a.equals(b))
    )
    assert(builder.stats.min.equals(ts1))
    assert(builder.stats.max.equals(ts3))
  }

  test("binary column") {
    val rowOI = LazyPrimitiveObjectInspectorFactory.LAZY_BINARY_OBJECT_INSPECTOR
    val binary1 = LazyFactory.createLazyPrimitiveClass(rowOI).asInstanceOf[LazyBinary]
    val ref1 = new ByteArrayRef
    val data = Array[Byte](0, 1, 2)
    ref1.setData(data)
    binary1.init(ref1, 0, 3)

    val builder = new BinaryColumnBuilder
    testColumn(
      Array[LazyBinary](binary1),
      builder,
      PrimitiveObjectInspectorFactory.writableBinaryObjectInspector,
      classOf[BinaryColumnIterator.Default],
      false,
      compareBinary)
    assert(builder.stats == null)
 
    def compareBinary(x: Object, y: Object): Boolean = {
      val xdata = x.asInstanceOf[ByteArrayRef].getData
      val ydata = y.asInstanceOf[LazyBinary].getWritableObject().getBytes()
      java.util.Arrays.equals(xdata, ydata)
    }
  }

  def testColumn[T, U <: ColumnIterator](
    testData: Seq[_ <: Object],
    builder: ColumnBuilder[T],
    writableOi: AbstractPrimitiveWritableObjectInspector,
    iteratorClass: Class[U],
    expectEWAHWrapper: Boolean = false,
    compareFunc: (Object, Object) => Boolean = (a, b) => a == b) {

    builder.initialize(5)
    testData.foreach { x =>
      if (x == null) builder.appendNull() else builder.append(x.asInstanceOf[T])
    }
    val buf = builder.build

    def executeOneTest() {
      val bufreader = ByteBufferReader.createUnsafeReader(buf)
      val columnType = bufreader.getLong().toInt

      val factory = ColumnIterator.getFactory(columnType)
      val iter = factory.createIterator(bufreader)
      //println(columnType + " " + iter.getClass)

      if (expectEWAHWrapper) {
        assert(iter.getClass === classOf[EWAHNullableColumnIterator[U]])
      } else {
        assert(iter.getClass === iteratorClass)
      }

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

