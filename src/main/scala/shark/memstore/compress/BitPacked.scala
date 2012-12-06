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

package shark.memstore.compress

/**
 * Store an array of integers using bit packing. We currently only pack into
 * 1, 2, 4, 8, or 16 bits. This can be used to efficiently store an int column
 * of ranges smaller than 32 bits, or used to represent dictionary encoded
 * columns.
 *
 * These classes are optimized for iterator-based reads.
 */
object BitPacked {

  /**
   * An array of elements all having the same constant value.
   */
  class BitPacked0(data: Array[Int], base: Int = 0) extends IntIterable {
    override val size: Int = data.size
    override def iterator = new IntIterator {
      val base = BitPacked0.this.base
      override def next = base
    }
  }

  /**
   * Packing 64 1-bit integers into a single long value.
   */
  class BitPacked1(data: Array[Int], base: Int = 0) extends IntIterable {
    override val size: Int = data.size

    private val packed: Array[Long] = {
      val packed = new Array[Long](math.ceil(data.size.toDouble / 64).toInt)
      var i = 0
      while (i < size) {
        packed(i / 64) |= ( (data(i) - base) & 1L ) << (i % 64)
        i += 1
      }
      packed
    }

    override def iterator = new IntIterator {
      // Making a class local "base" value gets us a 20% performance bump.
      val base = BitPacked1.this.base
      var offset = 0
      var index = 0
      var buf: Long = if (size > 0) packed(0) else 0L
      override def next = {
        // The branch predictor should pick to skip the if statement most of the time.
        if (offset == 64) {
          offset = 0
          index += 1
          buf = packed(index)
        }
        val v: Int = base + (( buf >> offset ) & 1L).toInt
        offset += 1
        v
      }
    }
  }

  /**
   * Packing 32 2-bit integers into a single long value.
   */
  class BitPacked2(data: Array[Int], base: Int = 0) extends IntIterable {
    override val size: Int = data.size

    private val packed: Array[Long] = {
      val packed = new Array[Long](math.ceil(data.size.toDouble / 32).toInt)
      var i = 0
      while (i < size) {
        packed(i / 32) |= ( (data(i) - base) & 3L ) << (i % 32 * 2)
        i += 1
      }
      packed
    }

    override def iterator = new IntIterator {
      // Making a class local "base" value gets us a 20% performance bump.
      val base = BitPacked2.this.base
      var offset = 0
      var index = 0
      var buf: Long = if (size > 0) packed(0) else 0L
      override def next = {
        if (offset == 64) {
          offset = 0
          index += 1
          buf = packed(index)
        }
        val v: Int = base + (( buf >> offset ) & 3L).toInt
        offset += 2
        v
      }
    }
  }

  /**
   * Packing 16 4-bit integers into a single long value.
   */
  class BitPacked4(data: Array[Int], base: Int = 0) extends IntIterable {
    override val size: Int = data.size

    private val packed: Array[Long] = {
      val packed = new Array[Long](math.ceil(data.size.toDouble / 16).toInt)
      var i = 0
      while (i < size) {
        packed(i / 16) |= ( (data(i) - base) & 15L ) << (i % 16 * 4)
        i += 1
      }
      packed
    }

    override def iterator = new IntIterator {
      // Making a class local "base" value gets us a 20% performance bump.
      val base = BitPacked4.this.base
      var offset = 0
      var index = 0
      var buf: Long = if (size > 0) packed(0) else 0L
      override def next = {
        if (offset == 64) {
          offset = 0
          index += 1
          buf = packed(index)
        }
        val v: Int = base + (( buf >> offset ) & 15L).toInt
        offset += 4
        v
      }
    }
  }

  /**
   * Use an byte array to represent integer arrays.
   */
  class BitPacked8(data: Array[Int], base: Int = 0) extends IntIterable {
    override val size: Int = data.size
    private val packed: Array[Byte] = {
      val packed = new Array[Byte](data.size)
      var i = 0
      while (i < size) {
        packed(i) = (data(i) - base - 128).toByte
        i += 1
      }
      packed
    }

    override def iterator = new IntIterator {
      var index = 0
      val base = BitPacked8.this.base + 128
      override def next = {
        val v: Int = packed(index).toInt + base
        index += 1
        v
      }
    }
  }

  /**
   * Use an short array to represent integer arrays.
   */
  class BitPacked16(data: Array[Int], base: Int = 0) extends IntIterable {
    override val size: Int = data.size
    private val packed: Array[Short] = {
      val packed = new Array[Short](data.size)
      var i = 0
      while (i < size) {
        packed(i) = (data(i) - base - 32768).toShort
        i += 1
      }
      packed
    }

    override def iterator = new IntIterator {
      var index = 0
      val base = BitPacked16.this.base + 32768
      override def next = {
        val v: Int = packed(index).toInt + base
        index += 1
        v
      }
    }
  }

  /**
   * A data structure that doesn't do any packing. For performance comparison only.
   */
  class Unpacked(val data: Array[Int]) extends IntIterable {
    override val size = data.size
    override def iterator = new IntIterator {
      var index = 0
      def next = {
        val v = data(index)
        index += 1
        v
      }
    }
  }
}
