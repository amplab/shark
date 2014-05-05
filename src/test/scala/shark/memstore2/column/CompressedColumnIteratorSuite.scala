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

package shark.memstore2.column

import java.nio.ByteBuffer
import java.nio.ByteOrder
import org.scalatest.FunSuite

import org.apache.hadoop.io.Text
import org.apache.hadoop.hive.serde2.objectinspector.primitive._
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector

import shark.memstore2.column.Implicits._

class CompressedColumnIteratorSuite extends FunSuite {

  /**
   * Generic tester across types and encodings. The function applies the given compression
   * algorithm on the given sequence of values, and test whether the resulting iterator gives
   * the same sequence of values.
   *
   * If we expect the compression algorithm to not compress the data, we should set the
   * shouldNotCompress flag to true. This way, it doesn't actually create a compressed buffer,
   * but simply tests the compression ratio returned by the algorithm is >= 1.0.
   */
  def testList[T, W](
      l: Seq[T],
      t: ColumnType[T, _],
      algo: CompressionAlgorithm,
      expectedCompressedSize: Long,
      shouldNotCompress: Boolean = false)
  {
    val b = ByteBuffer.allocate(1024 + (3 * 40 * l.size))
    b.order(ByteOrder.nativeOrder())
    b.putInt(t.typeID)
    l.foreach { item =>
      t.append(item, b)
      algo.gatherStatsForCompressibility(item, t.asInstanceOf[ColumnType[Any, _]])
    }
    b.limit(b.position())
    b.rewind()

    info("compressed size: %d, uncompressed size: %d, compression ratio %f".format(
      algo.compressedSize, algo.uncompressedSize, algo.compressionRatio))

    info("expected compressed size: %d".format(expectedCompressedSize))
    assert(algo.compressedSize === expectedCompressedSize)

    if (shouldNotCompress) {
      assert(algo.compressionRatio >= 1.0)
    } else {
      val compressedBuffer = algo.compress(b, t)
      val iter = new TestIterator(compressedBuffer, compressedBuffer.getInt())

      val oi: ObjectInspector = t match {
        case BOOLEAN => PrimitiveObjectInspectorFactory.writableBooleanObjectInspector
        case BYTE    => PrimitiveObjectInspectorFactory.writableByteObjectInspector
        case SHORT   => PrimitiveObjectInspectorFactory.writableShortObjectInspector
        case INT     => PrimitiveObjectInspectorFactory.writableIntObjectInspector
        case LONG    => PrimitiveObjectInspectorFactory.writableLongObjectInspector
        case STRING  => PrimitiveObjectInspectorFactory.writableStringObjectInspector
        case _       => throw new UnsupportedOperationException("Unsupported compression type " + t)
      }

      l.foreach { x =>
        assert(iter.hasNext)
        iter.next()
        assert(t.get(iter.current, oi) === x)
      }

      // Make sure we reach the end of the iterator.
      assert(!iter.hasNext)
    }
  }

  test("RLE Boolean") {
    // 3 runs: (1+4)*3
    val bools = Seq(true, true, false, true, true, true, true, true, true, true, true, true)
    testList(bools, BOOLEAN, new RLE, 15)
  }

  test("RLE Byte") {
    // 3 runs: (1+4)*3
    testList(Seq[Byte](10, 10, 10, 10, 10, 10, 10, 10, 10, 20, 10), BYTE, new RLE, 15)
  }

  test("RLE Short") {
    // 3 runs: (2+4)*3
    testList(Seq[Short](10, 10, 10, 20000, 20000, 20000, 500, 500, 500, 500), SHORT, new RLE, 18)
  }

  test("RLE Int") {
    // 3 runs: (4+4)*3
    testList(Seq[Int](1000000, 1000000, 1000000, 1000000, 900000, 99), INT, new RLE, 24)
  }

  test("RLE Long") {
    // 2 runs: (8+4)*3
    val longs = Seq[Long](2147483649L, 2147483649L, 2147483649L, 2147483649L, 500L, 500L, 500L)
    testList(longs, LONG, new RLE, 24)
  }

  test("RLE String") {
    // 3 runs: (4+4+4) + (4+1+4) + (4+1+4) = 30
    val strs: Seq[Text] = Seq("abcd", "abcd", "abcd", "e", "e", "!", "!").map(s => new Text(s))
    testList(strs, STRING, new RLE, 30)
  }

  test("Dictionary Encoded Int") {
    // dict len + 3 distinct values + 7 values = 4 + 3*4 + 7*2 = 30
    val ints = Seq[Int](1000000, 1000000, 99, 1000000, 1000000, 900000, 99)
    testList(ints, INT, new DictionaryEncoding, 30)
  }

  test("Dictionary Encoded Long") {
    // dict len + 2 distinct values + 7 values = 4 + 2*8 + 7*2 = 34
    val longs = Seq[Long](2147483649L, 2147483649L, 2147483649L, 2147483649L, 500L, 500L, 500L)
    testList(longs, LONG, new DictionaryEncoding, 34)
  }

  test("Dictionary Encoded String") {
    // dict len + 3 distinct values + 8 values = 4 + (4+4) + (4+1) + (4+1) + 8*2 =
    val strs: Seq[Text] = Seq("abcd", "abcd", "abcd", "e", "e", "e", "!", "!").map(s => new Text(s))
    testList(strs, STRING, new DictionaryEncoding, 38, shouldNotCompress = false)
  }

  test("Dictionary Encoding at limit of unique values") {
    val ints = Range(0, Short.MaxValue - 1).flatMap(i => Iterator(i, i, i))
    val expectedLen = 4 + (Short.MaxValue - 1) * 4 + 2 * (Short.MaxValue - 1) * 3
    testList(ints, INT, new DictionaryEncoding, expectedLen)
  }

  test("Dictionary Encoding - should not compress") {
    val ints = Range(0, Short.MaxValue.toInt)
    testList(ints, INT, new DictionaryEncoding, Int.MaxValue, shouldNotCompress = true)
  }

  test("RLE - should not compress") {
    val ints = Range(0, Short.MaxValue.toInt + 1)
    val expectedLen = (Short.MaxValue.toInt + 1) * (4 + 4)
    testList(ints, INT, new RLE, expectedLen, shouldNotCompress = true)
  }

  test("BooleanBitSet Boolean (shorter)") {
    // 1 Long worth of Booleans, in addition to the length field: 4+8
    val bools = Seq(true, true, false, false)
    testList(bools, BOOLEAN, new BooleanBitSetCompression, 4+8)
  }

  test("BooleanBitSet Boolean (longer)") {
    // 2 Longs worth of Booleans, in addition to the length field: 4+8+8
    val bools = Seq(true, true, false, false, true, true, false, false,true, true, false, false,true, true, false, false,
      true, true, false, false,true, true, false, false, true, true, false, false,true, true, false, false,
      true, true, false, false,true, true, false, false, true, true, false, false,true, true, false, false,
      true, true, false, false,true, true, false, false, true, true, false, false,true, true, false, false,
      true, true, false, false,true, true, false, false, true, true, false, false,true, true, false, false)
    testList(bools, BOOLEAN, new BooleanBitSetCompression, 4+8+8)
  }

  test("BooleanBitSet Boolean should not compress - compression ratio > 1") {
    // 1 Long worth of Booleans, in addtion to the length field: 4+8
    val bools = Seq(true, false)
    testList(bools, BOOLEAN, new BooleanBitSetCompression, 4+8, shouldNotCompress = true)
  }

  test("IntDeltaEncoding") {
    // base 5 + 4 small diffs + newBase 5 = 14
    val ints = Seq[Int](1000000, 1000001, 1000002, 1000003, 1000004, 5)
    testList(ints, INT, new IntDeltaEncoding, 5 + 4 + 5)

    val ints2 = Seq[Int](1000000, 1000001, 1000000, 1000004, 1000001, 5)
    testList(ints2, INT, new IntDeltaEncoding, 5 + 4 + 5)

    testList(List(0, 62),      INT, new IntDeltaEncoding, 1 + 4 + 1)
    testList(List(0, 63),      INT, new IntDeltaEncoding, 1 + 4 +  1)
    testList(List(0, 64),      INT, new IntDeltaEncoding, 1 + 4 + 1)
    testList(List(0, 63, 64),  INT, new IntDeltaEncoding, 1 + 4 + 1 + 1)
    testList(List(0, 128, -125), INT, new IntDeltaEncoding, 1 + 4 + 1 + 4 +  1 + 4)

    testList(List(0, 12400, 12600, 100, 228), INT, new IntDeltaEncoding, 5 * 5)
    testList(Range(-4, 0), INT, new IntDeltaEncoding, 1 + 4 + 3)

    val ints3 = Range(0, Byte.MaxValue.toInt - 1)
    testList(ints3, INT, new IntDeltaEncoding, 1 + 4 + 125)

    val ints4 = Range(Byte.MinValue.toInt + 2, 0)
    testList(ints4, INT, new IntDeltaEncoding, 1 + 4 + 125)
  }

  test("LongDeltaEncoding") {
    // base 9 + 3 small deltas + newBase 9 + 2 small deltas = 23
    val longs = Seq[Long](2147483649L, 2147483649L, 2147483649L, 2147483649L, 500L, 500L, 500L)
    testList(longs, LONG, new LongDeltaEncoding, 23)
  }

  test("int delta encoding boundary condition") {
    // 127 deltas are fine, while 128 are not.
    var skips = Range(0, 1000).map { x => -127 * x }
    testList(skips, INT, new IntDeltaEncoding, 1 + 4 + 999)

    skips = Range(0, 1000).map { x => 127 * x }
    testList(skips, INT, new IntDeltaEncoding, 1 + 4 + 999)

    skips = Range(0, 1000).map { x => 128 * x }
    testList(skips, INT, new IntDeltaEncoding, 1 + 4 + (5 * 999))

    skips = Range(0, 1000).map { x => -128 * x }
    testList(skips, INT, new IntDeltaEncoding, 1 + 4 + (5 * 999))
  }

  test("long delta encoding boundary condition") {
    // 127 deltas are fine, while 128 are not.
    var skips = Range(0, 1000).map { x => (-127 * x).toLong }
    testList(skips, LONG, new LongDeltaEncoding, 1 + 8 + 999)

    skips = Range(0, 1000).map { x => (127 * x).toLong }
    testList(skips, LONG, new LongDeltaEncoding, 1 + 8 + 999)

    skips = Range(0, 1000).map { x => (128 * x).toLong }
    testList(skips, LONG, new LongDeltaEncoding, 1 + 8 + (9 * 999))

    skips = Range(0, 1000).map { x => (-128 * x).toLong }
    testList(skips, LONG, new LongDeltaEncoding, 1 + 8 + (9 * 999))
  }
}

class TestIterator(val buffer: ByteBuffer, val columnType: ColumnType[_,_])
  extends CompressedColumnIterator
