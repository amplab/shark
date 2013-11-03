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

import java.lang.Integer.{ rotateLeft => rotl }

/**
 * <p>The MurmurHash3_x86_128(...) is a fast, non-cryptographic, 128-bit hash 
 * function that has excellent avalanche and 2-way bit independence properties. 
 * </p>
 * 
 * <p>The C++ version, revision 147, of the MurmurHash3, written Austin Appleby,
 * and which is in the Public Domain, was the inspiration for this 
 * implementation in Scala.  The C++ version can be found at 
 * <a href="http://code.google.com/p/smhasher">SMHasher & MurmurHash</a>.</p>
 * 
 * The Scala implementation follows the C++ version closely with two additional features
 * tailored for scenarios where object allocation is expensive., e.g where the hash function
 * is called several million times.
 * Use the method hash(data, seed, length) if you would like to reuse the same input buffer.
 * Likewise, use the method hash(data, seed, length, results) if you would like to reuse
 * the output buffer which is always of a fixed length 4.
 * 
 * 
 * @author Ram Sriharsha (harshars at yahoo-inc dot com)</p>
 */

sealed class HashState(var h1: Int, var h2: Int, var h3: Int, var h4: Int) {
  
  val C1 = 0x239b961b
  val C2 = 0xab0e9789
  val C3 = 0x38b34ae5 
  val C4 = 0xa1e38b93

  @inline final def blockMix(k1: Int, k2: Int, k3: Int, k4: Int) {
    h1 ^= selfMixK1(k1)
    h1 = rotl(h1, 19); h1 += h2; h1 = h1 * 5 + 0x561ccd1b
    h2 ^= selfMixK2(k2)
    h2 = rotl(h2, 17); h2 += h3; h2 = h2 * 5 + 0x0bcaa747
    h3 ^= selfMixK3(k3)
    h3 = rotl(h3, 15); h3 += h4; h3 = h3 * 5 + 0x96cd1c35
    h4 ^= selfMixK4(k4)
    h4 = rotl(h4, 13); h4 += h1; h4 = h4 * 5 + 0x32ac3b17
  }

  @inline final def finalMix(k1: Int, k2: Int, k3: Int, k4: Int, len: Int) {
    h1 ^= (if (k1 ==0) 0 else selfMixK1(k1))
    h2 ^= (if (k2 ==0) 0 else selfMixK2(k2))
    h3 ^= (if (k3 ==0) 0 else selfMixK3(k3))
    h4 ^= (if (k4 ==0) 0 else selfMixK4(k4))
    h1 ^= len; h2 ^= len; h3 ^= len; h4 ^= len

    h1 += h2; h1 += h3; h1 += h4
    h2 += h1; h3 += h1; h4 += h1

    h1 = fmix(h1)
    h2 = fmix(h2)
    h3 = fmix(h3)
    h4 = fmix(h4)

    h1 += h2; h1 += h3; h1 += h4
    h2 += h1; h3 += h1; h4 += h1
  }

  @inline final def fmix(hash: Int): Int = {
    var h = hash
    h ^= h >> 16
    h *= 0x85ebca6b
    h ^= h >> 13
    h *= 0xc2b2ae35
    h ^= h >> 16
    h
  }

  @inline final def selfMixK1(k: Int): Int = {
    var k1 = k; k1 *= C1; k1 = rotl(k1, 15); k1 *= C2
    k1
  }

  @inline final def selfMixK2(k: Int): Int = {
    var k2 = k; k2 *= C2; k2 = rotl(k2, 16); k2 *= C3
    k2
  }

  @inline final def selfMixK3(k: Int): Int = {
    var k3 = k; k3 *= C3; k3 = rotl(k3, 17); k3 *= C4
    k3
  }

  @inline final def selfMixK4(k: Int): Int = {
    var k4 = k; k4 *= C4; k4 = rotl(k4, 18); k4 *= C1
    k4
  }
}

object MurmurHash3_x86_128 {

  /**
   * @param data is the bytes to be hashed.
   * @param seed is the seed for the murmurhash algorithm.
   */
  @inline final def hash(data: Array[Byte], seed: Int)
  : Array[Int]  = {
    hash(data, seed, data.length)
  }

  /**
   * An optimization for reusing memory under large number of hash calls.
   * @param data is the bytes to be hashed.
   * @param seed is the seed for the murmurhash algorithm.
   * @param length is the length of the buffer to use for hashing.
   * @param results is the output buffer to store the four ints that are returned,
   *        should have size at least 4.
   */
  @inline final def hash(data: Array[Byte], seed: Int, length: Int,
      results: Array[Int]): Unit = {
     var i = 0
    val blocks = length >> 4
    val state = new HashState(seed, seed, seed, seed)
    while (i < blocks) {
      val k1 = getInt(data, 4*i, 4)
      val k2 = getInt(data, 4*i + 4, 4)
      val k3 = getInt(data, 4*i + 8, 4)
      val k4 = getInt(data, 4*i + 12, 4)
      state.blockMix(k1, k2, k3, k4)
      i += 1
    }
    var k1, k2, k3, k4 = 0
    val tail = blocks * 16
    val rem = length - tail
    // atmost 15 bytes remain
    rem match {
      case 12 | 13 | 14 | 15 => {
        k1 = getInt(data, tail, 4)
        k2 = getInt(data, tail + 4, 4)
        k3 = getInt(data, tail + 8, 4)
        k4 = getInt(data, tail + 12, rem - 12)
      }
      case 8 | 9 | 10 | 11 => {
        k1 = getInt(data, tail, 4)
        k2 = getInt(data, tail + 4, 4)
        k3 = getInt(data, tail + 8, rem - 8)
      }
      case 4 | 5 | 6 | 7 => {
        k1 = getInt(data, tail, 4)
        k2 = getInt(data, tail + 4, rem - 4)
      }
      case 0 | 1 | 2 | 3 => {
        k1 = getInt(data, tail, rem)
      }
    }
    state.finalMix(k1, k2, k3, k4, length)
    results(0) = state.h1
    results(1) = state.h2
    results(2) = state.h3
    results(3) = state.h4
  }

  /**
   * An optimization for reusing memory under large number of hash calls.
   * @param data is the bytes to be hashed.
   * @param seed is the seed for the murmurhash algorithm.
   * @param length is the length of the buffer to use for hashing.
   * @return is an array of size 4 that holds the four ints that comprise the 128 bit hash.
   */
  @inline final def hash(data: Array[Byte], seed: Int, length: Int)
  : Array[Int] = {
    val results = new Array[Int](4)
    hash(data, seed, length, results)
    results
  }
  
  /**
   * Utility function to convert a byte array into an int, filling in zeros
   * if the byte array is not big enough.
   * @param data is the byte array to be converted to an int.
   * @param index is the starting index in the byte array.
   * @param rem is the remainder of the byte array to examine.
   */
  @inline final def getInt(data: Array[Byte], index: Int, rem: Int): Int = {
    rem match {
      case 3 => data(index) << 24 | 
                (data(index + 1) & 0xFF) << 16 |
                (data(index + 2) & 0xFF) << 8
      case 2 => data(index) << 24 | 
                (data(index + 1) & 0xFF) << 16
      case 1 => data(index) << 24
      case 0 => 0
      case _ => data(index) << 24 | 
                (data(index + 1) & 0xFF) << 16 |
                (data(index + 2) & 0xFF) << 8 |
                (data(index + 3) & 0xFF)
    }
  }
}
