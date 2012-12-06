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

package shark.microbenchmarks

import scala.testing.Benchmark
import scala.util.Random

import shark.memstore.compress.BitPacked
import shark.memstore.compress.DeltaEncoded
import shark.memstore.compress.IntIterable


/**
 * A collection of micro-benchmarks used to measure the performance of integer
 * compression algorithms. This file measures the performance of bit packing
 * and delta encoding and compare them against unpacked version of integer arrays.
 *
 * The benchmarks can be invoked using the run script, e.g.:
 *   SHARK_MASTER_MEM=1g ./run shark.microbenchmarks.DeltaEncoding1 10
 */

object IntCompressionBenchmark {

  val DATA_SIZE = 50 * 1000 * 1000

  def generateTestArray(length: Int, numBits: Int, base: Int = 0): Array[Int] = {
    val maxValue = 1 << numBits
    val rand = new Random(43)
    Array.fill[Int](length){ rand.nextInt(maxValue) + base }
  }

  def generateDeltaEncodedArray(length: Int, numBits: Int, base: Int): Array[Int] = {
    val maxValue = 1 << numBits
    val rand = new Random(43)
    val data = new Array[Int](length)
    data(0) = base
    var i = 1
    while (i < length) {
      data(i) = data(i - 1) + rand.nextInt(maxValue)
      i += 1
    }
    data
  }

  def testRun(data: IntIterable) {
    val iter = data.iterator
    var sum: Int = 0
    var i = 0
    while (i < data.size) {
      sum += iter.next
      i += 1
    }
    println("total sum: " + sum)
  }
}

///////////////////////////////////////////////////////////////////////////////
// Delta encoding
///////////////////////////////////////////////////////////////////////////////

object DeltaEncoding1 extends Benchmark {
  var data = new DeltaEncoded.DeltaEncoded1(
    IntCompressionBenchmark.generateDeltaEncodedArray(IntCompressionBenchmark.DATA_SIZE, 1, 100))
  override def run = IntCompressionBenchmark.testRun(data)
}

object DeltaEncoding2 extends Benchmark {
  var data = new DeltaEncoded.DeltaEncoded2(
    IntCompressionBenchmark.generateDeltaEncodedArray(IntCompressionBenchmark.DATA_SIZE, 2, 100))
  override def run = IntCompressionBenchmark.testRun(data)
}

object DeltaEncoding4 extends Benchmark {
  var data = new DeltaEncoded.DeltaEncoded4(
    IntCompressionBenchmark.generateDeltaEncodedArray(IntCompressionBenchmark.DATA_SIZE, 4, 100))
  override def run = IntCompressionBenchmark.testRun(data)
}

object DeltaEncoding8 extends Benchmark {
  var data = new DeltaEncoded.DeltaEncoded8(
    IntCompressionBenchmark.generateDeltaEncodedArray(IntCompressionBenchmark.DATA_SIZE, 8, 100))
  override def run = IntCompressionBenchmark.testRun(data)
}

object DeltaEncoding16 extends Benchmark {
  var data = new DeltaEncoded.DeltaEncoded16(
    IntCompressionBenchmark.generateDeltaEncodedArray(IntCompressionBenchmark.DATA_SIZE, 8, 100))
  override def run = IntCompressionBenchmark.testRun(data)
}

///////////////////////////////////////////////////////////////////////////////
// Bit packing
///////////////////////////////////////////////////////////////////////////////

object BitPacking1 extends Benchmark {
  var data = new BitPacked.BitPacked1(
    IntCompressionBenchmark.generateTestArray(IntCompressionBenchmark.DATA_SIZE, 1))

  override def run = IntCompressionBenchmark.testRun(data)
}

object BitPacking2 extends Benchmark {
  var data = new BitPacked.BitPacked2(
    IntCompressionBenchmark.generateTestArray(IntCompressionBenchmark.DATA_SIZE, 2))

  override def run = IntCompressionBenchmark.testRun(data)
}

object BitPacking4 extends Benchmark {
  var data = new BitPacked.BitPacked4(
    IntCompressionBenchmark.generateTestArray(IntCompressionBenchmark.DATA_SIZE, 4))

  override def run = IntCompressionBenchmark.testRun(data)
}

object BitPacking8 extends Benchmark {
  var data = new BitPacked.BitPacked8(
    IntCompressionBenchmark.generateTestArray(IntCompressionBenchmark.DATA_SIZE, 8))

  override def run = IntCompressionBenchmark.testRun(data)
}

object BitPacking16 extends Benchmark {
  var data = new BitPacked.BitPacked16(
    IntCompressionBenchmark.generateTestArray(IntCompressionBenchmark.DATA_SIZE, 16))

  override def run = IntCompressionBenchmark.testRun(data)
}

///////////////////////////////////////////////////////////////////////////////
// Unpacked
///////////////////////////////////////////////////////////////////////////////

object Unpacked1 extends Benchmark {
  var data = new BitPacked.Unpacked(
    IntCompressionBenchmark.generateTestArray(IntCompressionBenchmark.DATA_SIZE, 1))

  override def run = IntCompressionBenchmark.testRun(data)
}

object Unpacked2 extends Benchmark {
  var data = new BitPacked.Unpacked(
    IntCompressionBenchmark.generateTestArray(IntCompressionBenchmark.DATA_SIZE, 2))

  override def run = IntCompressionBenchmark.testRun(data)
}

object Unpacked4 extends Benchmark {
  var data = new BitPacked.Unpacked(
    IntCompressionBenchmark.generateTestArray(IntCompressionBenchmark.DATA_SIZE, 4))

  override def run = IntCompressionBenchmark.testRun(data)
}

object Unpacked8 extends Benchmark {
  var data = new BitPacked.Unpacked(
    IntCompressionBenchmark.generateTestArray(IntCompressionBenchmark.DATA_SIZE, 8))

  override def run = IntCompressionBenchmark.testRun(data)
}

object Unpacked16 extends Benchmark {
  var data = new BitPacked.Unpacked(
    IntCompressionBenchmark.generateTestArray(IntCompressionBenchmark.DATA_SIZE, 16))

  override def run = IntCompressionBenchmark.testRun(data)
}

object Unpacked extends Benchmark {
  var data = new BitPacked.Unpacked(
    IntCompressionBenchmark.generateDeltaEncodedArray(IntCompressionBenchmark.DATA_SIZE, 1, 100))
  override def run = IntCompressionBenchmark.testRun(data)
}
