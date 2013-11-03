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

package shark.util

import java.util.BitSet
import java.nio.charset.Charset

import scala.math.{ceil, log}

import com.google.common.primitives.Ints
import com.google.common.primitives.Longs

/**
 * Bloom Filter
 * <a href="http://www.eecs.harvard.edu/~michaelm/NEWWORK/postscripts/BloomFilterSurvey.pdf">
 * MitzenMacher</a>
 * @constructor create a bloom filter.
 * @param numBitsPerElement is the expected number of bits per element.
 * @param expectedSize is the number of elements to be contained in the filter.
 * @param numHashes is the number of hash functions.
 * @author Ram Sriharsha (harshars at yahoo-inc dot com)
 */
class BloomFilter(numBitsPerElement: Double, expectedSize: Int, numHashes: Int) 
  extends AnyRef with Serializable {

  val SEED = System.getProperty("shark.bloomfilter.seed","1234567890").toInt
  val bitSetSize = math.ceil(numBitsPerElement * expectedSize).toInt
  val bitSet = new BitSet(bitSetSize)

  /**
   * @param fpp is the expected false positive probability.
   * @param expectedSize is the number of elements to be contained.
   */
  def this(fpp: Double, expectedSize: Int) {
   this(BloomFilter.numBits(fpp, expectedSize), 
       expectedSize, 
       BloomFilter.numHashes(fpp, expectedSize))
  }

  /**
   * @param data is the bytes to be hashed.
   */
  def add(data: Array[Byte]) {
    val hashes = hash(data, numHashes)
    var i = hashes.size
    while (i > 0) {
      i -= 1
      bitSet.set(hashes(i) % bitSetSize, true)
    }
  }

  /**
   * Optimization to allow reusing the same input buffer by specifying
   * the length of the buffer that contains the bytes to be hashed.
   * @param data is the bytes to be hashed.
   * @param len is the length of the buffer to examine.
   */
  def add(data: Array[Byte], len: Int) {
    val hashes = hash(data, numHashes, len)
    var i = hashes.size
    while (i > 0) {
      i -= 1
      bitSet.set(hashes(i) % bitSetSize, true)
    }
  }

  def add(data: String, charset: Charset=Charset.forName("UTF-8")) {
    add(data.getBytes(charset))
  }

  def add(data: Int) {
    add(Ints.toByteArray(data))
  }
  
  def add(data: Long) {
    add(Longs.toByteArray(data))
  }

  def contains(data: String, charset: Charset=Charset.forName("UTF-8")): Boolean = {
    contains(data.getBytes(charset))
  }

  def contains(data: Int): Boolean = {
    contains(Ints.toByteArray(data))
  }
  
  def contains(data: Long): Boolean = {
    contains(Longs.toByteArray(data))
  }

  def contains(data: Array[Byte]): Boolean = {
    !hash(data,numHashes).exists {
      h => !bitSet.get(h % bitSetSize)
    } 
  }
  
  /**
   * Optimization to allow reusing the same input buffer by specifying
   * the length of the buffer that contains the bytes to be hashed.
   * @param data is the bytes to be hashed.
   * @param len is the length of the buffer to examine.
   * @return true with some false positive probability and false if the
   *         bytes is not contained in the bloom filter.
   */
  def contains(data: Array[Byte], len: Int): Boolean = {
    !hash(data,numHashes, len).exists {
      h => !bitSet.get(h % bitSetSize)
    } 
  }

  private def hash(data: Array[Byte], n: Int): Seq[Int] = {
    hash(data, n, data.length)
  }

  private def hash(data: Array[Byte], n: Int, len: Int): Seq[Int] = {
    val s = n >> 2
    val a = new Array[Int](n)
    var i = 0
    val results = new Array[Int](4)
    while (i < s) {
      MurmurHash3_x86_128.hash(data, SEED + i, len, results)
      a(i) = results(0).abs
      var j = i + 1
      if (j < n) {
        a(j) = results(1).abs
      }
      j += 1
      if (j < n) {
        a(j) = results(2).abs
      }
      j += 1
      if (j < n) {
        a(j) = results(3).abs
      }
      i += 1
    }
    a
  }
}

object BloomFilter {
  
  def numBits(fpp: Double, expectedSize: Int) = ceil(-(log(fpp) / log(2))) / log(2)
  
  def numHashes(fpp: Double, expectedSize: Int) = ceil(-(log(fpp) / log(2))).toInt

}
