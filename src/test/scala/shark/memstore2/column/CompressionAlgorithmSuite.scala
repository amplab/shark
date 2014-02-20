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

import java.nio.{ByteBuffer, ByteOrder}

import scala.collection.mutable.HashMap

import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory
import org.apache.hadoop.io.Text

import org.scalatest.FunSuite

import shark.memstore2.column.ColumnStats._

class CompressionAlgorithmSuite extends FunSuite {

  // TODO: clean these tests.

  test("CompressedColumnBuilder using RLE") {

    class TestColumnBuilder(val stats: ColumnStats[Int], val t: ColumnType[Int,_])
      extends CompressedColumnBuilder[Int] {
      override def shouldApply(scheme: CompressionAlgorithm) = true
    }

    val b = new TestColumnBuilder(new NoOpStats, INT)
    b.setCompressionSchemes(new RLE)
    b.initialize(100)
    val oi = PrimitiveObjectInspectorFactory.javaIntObjectInspector
    b.append(123.asInstanceOf[Object], oi)
    b.append(123.asInstanceOf[Object], oi)
    b.append(56.asInstanceOf[Object], oi)
    b.append(56.asInstanceOf[Object], oi)
    val compressedBuffer = b.build()
    assert(compressedBuffer.getInt() == INT.typeID)
    assert(compressedBuffer.getInt() == RLECompressionType.typeID)

    assert(compressedBuffer.getInt() == 123)
    assert(compressedBuffer.getInt() == 2)
    assert(compressedBuffer.getInt() == 56)
    assert(compressedBuffer.getInt() == 2)
    assert(!compressedBuffer.hasRemaining)
  }

  test("RLE Strings") {
    val b = ByteBuffer.allocate(1024)
    b.order(ByteOrder.nativeOrder())
    b.putInt(STRING.typeID)
    val rle = new RLE()
    Seq[Text](new Text("abc"), new Text("abc"), new Text("efg"), new Text("abc")).foreach { text =>
      STRING.append(text, b)
      rle.gatherStatsForCompressibility(text, STRING)
    }
    b.limit(b.position())
    b.rewind()
    val compressedBuffer = rle.compress(b, STRING)
    assert(compressedBuffer.getInt() == STRING.typeID)
    assert(compressedBuffer.getInt() == RLECompressionType.typeID)
    assert(STRING.extract(compressedBuffer).equals(new Text("abc")))
    assert(compressedBuffer.getInt() == 2)
    assert(STRING.extract(compressedBuffer).equals(new Text("efg")))
    assert(compressedBuffer.getInt() == 1)
    assert(STRING.extract(compressedBuffer).equals(new Text("abc")))
    assert(compressedBuffer.getInt() == 1)
    assert(!compressedBuffer.hasRemaining)
  }

  test("RLE int with run length 1") {
    val b = ByteBuffer.allocate(16)
    b.order(ByteOrder.nativeOrder())
    b.putInt(INT.typeID)
    b.putInt(123)
    b.putInt(56)
    b.limit(b.position())
    b.rewind()
    val rle = new RLE()
    rle.gatherStatsForCompressibility(123, INT)
    rle.gatherStatsForCompressibility(56, INT)
    val compressedBuffer = rle.compress(b, INT)
    assert(compressedBuffer.getInt() == INT.typeID)
    assert(compressedBuffer.getInt() == RLECompressionType.typeID)
    assert(compressedBuffer.getInt() == 123)
    assert(compressedBuffer.getInt() == 1)
    assert(compressedBuffer.getInt() == 56)
    assert(compressedBuffer.getInt() == 1)
    assert(!compressedBuffer.hasRemaining)
  }

  test("RLE int single run") {
    val b = ByteBuffer.allocate(4008)
    b.order(ByteOrder.nativeOrder())
    b.putInt(INT.typeID)
    val rle = new RLE()
    Range(0, 1000).foreach { x =>
      b.putInt(6)
      rle.gatherStatsForCompressibility(6, INT)
    }
    b.limit(b.position())
    b.rewind()
    val compressedBuffer = rle.compress(b, INT)
    assert(compressedBuffer.getInt() == INT.typeID)
    assert(compressedBuffer.getInt() == RLECompressionType.typeID)
    assert(compressedBuffer.getInt() == 6)
    assert(compressedBuffer.getInt() == 1000)
    assert(!compressedBuffer.hasRemaining)
  }

  test("RLE long single run") {
    val b = ByteBuffer.allocate(8008)
    b.order(ByteOrder.nativeOrder())
    b.putInt(LONG.typeID)
    val rle = new RLE()
    Range(0, 1000).foreach { x =>
      b.putLong(Long.MaxValue - 6)
      rle.gatherStatsForCompressibility(Long.MaxValue - 6, LONG)
    }
    b.limit(b.position())
    b.rewind()
    val compressedBuffer = rle.compress(b, LONG)
    assert(compressedBuffer.getInt() == LONG.typeID)
    assert(compressedBuffer.getInt() == RLECompressionType.typeID)
    assert(compressedBuffer.getLong() == Long.MaxValue - 6)
    assert(compressedBuffer.getInt() == 1000)
    assert(!compressedBuffer.hasRemaining)
  }

  test("RLE int 3 runs") {
    val b = ByteBuffer.allocate(4008)
    b.order(ByteOrder.nativeOrder())
    b.putInt(INT.typeID)
    val items = Array[Int](10, 20, 40)
    val rle = new RLE()

    Range(0, 1000).foreach { x =>
      val v = if (x < 100) items(0) else if (x < 500) items(1) else items(2)
      b.putInt(v)
      rle.gatherStatsForCompressibility(v, INT)
    }
    b.limit(b.position())
    b.rewind()
    val compressedBuffer = rle.compress(b, INT)
    assert(compressedBuffer.getInt() == INT.typeID)
    assert(compressedBuffer.getInt() == RLECompressionType.typeID)
    assert(compressedBuffer.getInt() == 10)
    assert(compressedBuffer.getInt() == 100)
    assert(compressedBuffer.getInt() == 20)
    assert(compressedBuffer.getInt() == 400)
    assert(compressedBuffer.getInt() == 40)
    assert(compressedBuffer.getInt() == 500)
    assert(!compressedBuffer.hasRemaining)
  }

  test("RLE int single long run") {
    val b = ByteBuffer.allocate(4000008)
    b.order(ByteOrder.nativeOrder())
    b.putInt(INT.typeID)
    val rle = new RLE()

    Range(0, 1000000).foreach { x =>
      b.putInt(6)
      rle.gatherStatsForCompressibility(6, INT)
    }
    b.limit(b.position())
    b.rewind()
    val compressedBuffer = rle.compress(b, INT)
    assert(compressedBuffer.getInt() == RLECompressionType.typeID)
    assert(compressedBuffer.getInt() == INT.typeID)
    assert(compressedBuffer.getInt() == 6)
    assert(compressedBuffer.getInt() == 1000000)
    assert(!compressedBuffer.hasRemaining)
  }

  test("IntDeltaEncoding") {
    val b = ByteBuffer.allocate(1024)
    b.order(ByteOrder.nativeOrder())
    b.putInt(INT.typeID)

    val bde = new IntDeltaEncoding

    val x = 1
    b.putInt(x)
    bde.gatherStatsForCompressibility(x, INT)

    val y = x + 40000
    b.putInt(y)
    bde.gatherStatsForCompressibility(y, INT)

    val z = y + 1
    b.putInt(z)
    bde.gatherStatsForCompressibility(z, INT)

    b.limit(b.position())
    b.rewind()
    val compressedBuffer = bde.compress(b, INT)
    assert(compressedBuffer.getInt() == INT.typeID)
    assert(compressedBuffer.getInt() == IntDeltaCompressionType.typeID)

    compressedBuffer.get() // first flagByte
    assert(INT.extract(compressedBuffer).equals(x))

    compressedBuffer.get() // second flagByte
    assert(INT.extract(compressedBuffer).equals(y))

    val seven: Byte = compressedBuffer.get() // third flagByte
    assert(seven === 1.toByte)

    assert(!compressedBuffer.hasRemaining)
  }

  test("LongDeltaEncoding") {
    val b = ByteBuffer.allocate(10024)
    b.order(ByteOrder.nativeOrder())
    b.putInt(LONG.typeID)

    val bde = new LongDeltaEncoding

    val x: Long = 1
    b.putLong(x)
    bde.gatherStatsForCompressibility(x, LONG)

    val y: Long = x + 40000
    b.putLong(y)
    bde.gatherStatsForCompressibility(y, LONG)

    val z: Long = y + 1
    b.putLong(z)
    bde.gatherStatsForCompressibility(z, LONG)

    b.limit(b.position())
    b.rewind()
    val compressedBuffer = bde.compress(b, LONG)
    assert(compressedBuffer.getInt() === LONG.typeID)
    assert(compressedBuffer.getInt() === LongDeltaCompressionType.typeID)

    compressedBuffer.get() // first flagByte
    assert(LONG.extract(compressedBuffer).equals(x))

    compressedBuffer.get() // second flagByte
    assert(LONG.extract(compressedBuffer).equals(y))

    val seven: Byte = compressedBuffer.get() // third flagByte
    assert(seven === 1.toByte)

    assert(!compressedBuffer.hasRemaining)
  }

  test("Dictionary Encoding") {

    def testList[T](
      l: Seq[T],
      u: ColumnType[T, _],
      expectedDictSize: Int,
      compareFunc: (T, T) => Boolean = (a: T, b: T) => a == b) {

      val b = ByteBuffer.allocate(1024 + (3*40*l.size))
      b.order(ByteOrder.nativeOrder())
      b.putInt(u.typeID)
      val de = new DictionaryEncoding()
      l.foreach { item =>
        assert(de.supportsType(u))
        u.append(item, b)
        de.gatherStatsForCompressibility(item, u)
      }
      b.limit(b.position())
      b.rewind()
      val compressedBuffer = de.compress(b, u)
      assert(compressedBuffer.getInt() === u.typeID)
      assert(compressedBuffer.getInt() === DictionaryCompressionType.typeID)
      assert(compressedBuffer.getInt() === expectedDictSize) //dictionary size
      val dictionary = new HashMap[Short, T]()
      var count = 0
      while (count < expectedDictSize) {
        val v = u.extract(compressedBuffer)
        dictionary.put(dictionary.size.toShort, u.clone(v))
        count += 1
      }
      assert(dictionary.get(0).get.equals(l(0)))
      assert(dictionary.get(1).get.equals(l(2)))
      l.foreach { x =>
        val y = dictionary.get(compressedBuffer.getShort()).get
        assert(compareFunc(y, x))
      }
    }

    val iList = Array[Int](10, 10, 20, 10)
    val lList = iList.map { i => Long.MaxValue - i.toLong }
    val sList = iList.map { i => new Text(i.toString) }

    testList(iList, INT, 2)
    testList(lList, LONG, 2)
    testList(sList, STRING, 2, (a: Text, b: Text) => a.hashCode == b.hashCode)

    // test at limit of unique values
    val alternating = Range(0, Short.MaxValue-1, 1).flatMap { s => List(1, s) }
    val longList = List.concat(iList, alternating, iList)
    assert(longList.size === (8 + 2*(Short.MaxValue-1)))
    testList(longList, INT, Short.MaxValue - 1)
  }

  test("Uncompressed text") {
    val b = new StringColumnBuilder
    b.initialize(0)
    val oi = PrimitiveObjectInspectorFactory.javaStringObjectInspector

    val lines = Array[String](
      "lar deposits. blithely final packages cajole. regular waters are final requests.",
      "hs use ironic, even requests. s",
      "ges. thinly even pinto beans ca",
      "ly final courts cajole furiously final excuse",
      "uickly special accounts cajole carefully blithely close requests. carefully final"
    )
    lines.foreach { line =>
      b.append(line, oi)
    }
    val newBuffer = b.build()
    assert(newBuffer.getInt() === 0)  // null count
    assert(newBuffer.getInt() === STRING.typeID)
    assert(newBuffer.getInt() === DefaultCompressionType.typeID)
  }

  test("BooleanBitSet encoding") {
    val bbs = new BooleanBitSetCompression()
    val b = ByteBuffer.allocate(4 + 64 + 2)
    b.order(ByteOrder.nativeOrder())
    b.putInt(BOOLEAN.typeID)
    for(_ <- 1 to 5) {
      b.put(0.toByte)
      b.put(1.toByte)
      bbs.gatherStatsForCompressibility(false, BOOLEAN)
      bbs.gatherStatsForCompressibility(true, BOOLEAN)
    }
    for(_ <- 1 to 54) {
      b.put(0.toByte)
      bbs.gatherStatsForCompressibility(false, BOOLEAN)
    }
    b.put(0.toByte)
    b.put(1.toByte)
    bbs.gatherStatsForCompressibility(false, BOOLEAN)
    bbs.gatherStatsForCompressibility(true, BOOLEAN)
    b.limit(b.position())
    b.rewind()
    val compressedBuffer = bbs.compress(b, BOOLEAN)
    assert(compressedBuffer.getInt() === BOOLEAN.typeID)
    assert(compressedBuffer.getInt() === BooleanBitSetCompressionType.typeID)
    assert(compressedBuffer.getInt() === 64 + 2)
    assert(compressedBuffer.getLong() === 682)
    assert(compressedBuffer.getLong() === 2)
    assert(!compressedBuffer.hasRemaining)
  }
}
