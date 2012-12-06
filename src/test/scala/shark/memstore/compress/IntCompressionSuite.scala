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

import scala.util.Random
import org.scalatest.FunSuite
import shark.memstore.compress._


class IntCompressionSuite extends FunSuite {

  /////////////////////////////////////////////////////////////////////////////
  // Bit packing section
  /////////////////////////////////////////////////////////////////////////////

  test("Bit packing 0 bit") {
    compare((new BitPacked.BitPacked1(testData(0))).iterator, testData(0))
    compare((new BitPacked.BitPacked1(testData2(0), BASE)).iterator, testData2(0))
  }

  test("Bit packing 1 bit") {
    (0 to 1).foreach { numBits =>
      compare((new BitPacked.BitPacked1(testData(numBits))).iterator, testData(numBits))
    }
    (0 to 1).foreach { numBits =>
      compare((new BitPacked.BitPacked1(testData2(numBits), BASE)).iterator, testData2(numBits))
    }
  }

  test("Bit packing 2 bit") {
    (0 to 2).foreach { numBits =>
      compare((new BitPacked.BitPacked2(testData(numBits))).iterator, testData(numBits))
    }
    (0 to 2).foreach { numBits =>
      compare((new BitPacked.BitPacked2(testData2(numBits), BASE)).iterator, testData2(numBits))
    }
  }

  test("Bit packing 4 bit") {
    (0 to 4).foreach { numBits =>
      compare((new BitPacked.BitPacked4(testData(numBits))).iterator, testData(numBits))
    }
    (0 to 4).foreach { numBits =>
      compare((new BitPacked.BitPacked4(testData2(numBits), BASE)).iterator, testData2(numBits))
    }
  }

  test("Bit packing 8 bit") {
    (0 to 8).foreach { numBits =>
      compare((new BitPacked.BitPacked8(testData(numBits))).iterator, testData(numBits))
    }
    (0 to 8).foreach { numBits =>
      compare((new BitPacked.BitPacked8(testData2(numBits), BASE)).iterator, testData2(numBits))
    }
  }

  test("Bit packing 16 bit") {
    (0 to 16).foreach { numBits =>
      compare((new BitPacked.BitPacked16(testData(numBits))).iterator, testData(numBits))
    }
    (0 to 16).foreach { numBits =>
      compare((new BitPacked.BitPacked16(testData2(numBits), BASE)).iterator, testData2(numBits))
    }
  }

  /////////////////////////////////////////////////////////////////////////////
  // Delta encoding section
  /////////////////////////////////////////////////////////////////////////////

  test("Delta encoding 1 bit") {
    (0 to 1).foreach { numBits =>
      compare(
        (new DeltaEncoded.DeltaEncoded1(testDataDeltaEncoded(numBits))).iterator,
        testDataDeltaEncoded(numBits))
    }
    (0 to 1).foreach { numBits =>
      compare(
        (new DeltaEncoded.DeltaEncoded1(testDataDeltaEncoded2(numBits))).iterator,
        testDataDeltaEncoded2(numBits))
    }
  }

  test("Delta encoding 2 bit") {
    (0 to 2).foreach { numBits =>
      compare(
        (new DeltaEncoded.DeltaEncoded2(testDataDeltaEncoded(numBits))).iterator,
        testDataDeltaEncoded(numBits))
    }
    (0 to 2).foreach { numBits =>
      compare(
        (new DeltaEncoded.DeltaEncoded2(testDataDeltaEncoded2(numBits))).iterator,
        testDataDeltaEncoded2(numBits))
    }
  }

  test("Delta encoding 4 bit") {
    (0 to 4).foreach { numBits =>
      compare(
        (new DeltaEncoded.DeltaEncoded4(testDataDeltaEncoded(numBits))).iterator,
        testDataDeltaEncoded(numBits))
    }
    (0 to 4).foreach { numBits =>
      compare(
        (new DeltaEncoded.DeltaEncoded4(testDataDeltaEncoded2(numBits))).iterator,
        testDataDeltaEncoded2(numBits))
    }
  }

  test("Delta encoding 8 bit") {
    (0 to 8).foreach { numBits =>
      compare(
        (new DeltaEncoded.DeltaEncoded8(testDataDeltaEncoded(numBits))).iterator,
        testDataDeltaEncoded(numBits))
    }
    (0 to 8).foreach { numBits =>
      compare(
        (new DeltaEncoded.DeltaEncoded8(testDataDeltaEncoded2(numBits))).iterator,
        testDataDeltaEncoded2(numBits))
    }
  }

  test("Delta encoding 16 bit") {
    (0 to 16).foreach { numBits =>
      compare(
        (new DeltaEncoded.DeltaEncoded16(testDataDeltaEncoded(numBits))).iterator,
        testDataDeltaEncoded(numBits))
    }
    (0 to 16).foreach { numBits =>
      compare(
        (new DeltaEncoded.DeltaEncoded16(testDataDeltaEncoded2(numBits))).iterator,
        testDataDeltaEncoded2(numBits))
    }
  }

  /////////////////////////////////////////////////////////////////////////////
  // Testing helper section
  /////////////////////////////////////////////////////////////////////////////

  def compare(iter: IntIterator, expected: Array[Int]) {
    expected.zipWithIndex.foreach { case(number, index) =>
      val v = iter.next
      assert(v == number, "position %d expected %d saw %d".format(index, number, v))
    }
  }

  val ARRAY_LENGTH = 512
  val BASE = 100

  val testData = Array.tabulate[Array[Int]](31) { numBits =>
    val maxValue = 1 << numBits
    val rand = new Random
    Array.fill[Int](ARRAY_LENGTH){ rand.nextInt(maxValue) }
  }

  val testData2 = Array.tabulate[Array[Int]](31) { numBits =>
    val maxValue = 1 << numBits
    val rand = new Random
    Array.fill[Int](ARRAY_LENGTH){ rand.nextInt(maxValue) + BASE }
  }

  // An array of data that can be delta encoded. The first value is 0.
  val testDataDeltaEncoded = Array.tabulate[Array[Int]](31) { numBits =>
    val maxValue = 1 << numBits
    val rand = new Random
    val arr = new Array[Int](ARRAY_LENGTH)
    arr(0) = 0
    var i = 1
    while (i < ARRAY_LENGTH) {
      arr(i) = arr(i - 1) + rand.nextInt(maxValue)
      i += 1
    }
    arr
  }

  // An array of data that can be delta encoded. The first value is BASE.
  val testDataDeltaEncoded2 = Array.tabulate[Array[Int]](31) { numBits =>
    val maxValue = 1 << numBits
    val rand = new Random
    val arr = new Array[Int](ARRAY_LENGTH)
    arr(0) = BASE
    var i = 1
    while (i < ARRAY_LENGTH) {
      arr(i) = arr(i - 1) + rand.nextInt(maxValue)
      i += 1
    }
    arr
  }
}
