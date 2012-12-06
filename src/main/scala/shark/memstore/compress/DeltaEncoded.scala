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
 * Store an array of integers using delta encoding, i.e. each element only
 * stores the increment from the previous element. The differences are then
 * stored using bit packing, which are currently packed into 1, 2, 4, 8, or
 * 16 bits.
 *
 * These classes are optimized for iterator-based reads.
 */
object DeltaEncoded {

  /**
   * Delta encoding using 1 bit per item.
   */
  class DeltaEncoded1(data: Array[Int]) extends IntIterable {
    override val size: Int = data.size

    val base: Int = if (data.size > 0) data(0) else 0

    private val packed: Array[Long] = {
      val packed = new Array[Long](math.ceil(data.size.toDouble / 64).toInt)
      var i = 0
      var last = base
      while (i < size) {
        packed(i / 64) |= ( (data(i) - last) & 1L ) << (i % 64)
        last = data(i)
        i += 1
      }
      packed
    }

    override def iterator = new IntIterator {
      var offset = 0
      var index = 0
      var base = DeltaEncoded1.this.base
      var buf: Long = if (size > 0) packed(0) else 0L
      override def next = {
        if (offset == 64) {
          offset = 0
          index += 1
          buf = packed(index)
        }
        base += (( buf >> offset ) & 1L).toInt
        offset += 1
        base
      }
    }
  }

  /**
   * Delta encoding using 2 bits per item.
   */
  class DeltaEncoded2(data: Array[Int]) extends IntIterable {
    override val size: Int = data.size

    val base: Int = if (data.size > 0) data(0) else 0

    private val packed: Array[Long] = {
      val packed = new Array[Long](math.ceil(data.size.toDouble / 32).toInt)
      var i = 0
      var last = base
      while (i < size) {
        packed(i / 32) |= ( (data(i) - last) & 3L ) << (i % 32 * 2)
        last = data(i)
        i += 1
      }
      packed
    }

    override def iterator = new IntIterator {
      var offset = 0
      var index = 0
      var base = DeltaEncoded2.this.base
      var buf: Long = if (size > 0) packed(0) else 0L
      override def next = {
        if (offset == 64) {
          offset = 0
          index += 1
          buf = packed(index)
        }
        base += (( buf >> offset ) & 3L).toInt
        offset += 2
        base
      }
    }
  }

  /**
   * Delta encoding using 4 bits per item.
   */
  class DeltaEncoded4(data: Array[Int]) extends IntIterable {
    override val size: Int = data.size

    val base: Int = if (data.size > 0) data(0) else 0

    private val packed: Array[Long] = {
      val packed = new Array[Long](math.ceil(data.size.toDouble / 16).toInt)
      var i = 0
      var last = base
      while (i < size) {
        packed(i / 16) |= ( (data(i) - last) & 15L ) << (i % 16 * 4)
        last = data(i)
        i += 1
      }
      packed
    }

    override def iterator = new IntIterator {
      var offset = 0
      var index = 0
      var base = DeltaEncoded4.this.base
      var buf: Long = if (size > 0) packed(0) else 0L
      override def next = {
        if (offset == 64) {
          offset = 0
          index += 1
          buf = packed(index)
        }
        base += (( buf >> offset ) & 15L).toInt
        offset += 4
        base
      }
    }
  }

  /**
   * Delta encoding using 1 byte (8 bits) per item.
   */
  class DeltaEncoded8(data: Array[Int]) extends IntIterable {
    override val size: Int = data.size

    val base: Int = if (data.size > 0) data(0) else 0

    // Delta of 0 is stored as -128.
    private val packed: Array[Byte] = {
      val packed = new Array[Byte](size)
      var i = 1
      var last = base
      packed(0) = -128.toByte
      while (i < size) {
        packed(i) = (data(i) - last - 128).toByte
        last = data(i)
        i += 1
      }
      packed
    }

    override def iterator = new IntIterator {
      var index = 0
      var base: Int = DeltaEncoded8.this.base
      override def next = {
        base += packed(index).toInt + 128
        index += 1
        base
      }
    }
  }

  /**
   * Delta encoding using 2 bytes (i.e. 1 short, 16 bits) per item.
   */
  class DeltaEncoded16(data: Array[Int]) extends IntIterable {
    override val size: Int = data.size

    val base: Int = if (data.size > 0) data(0) else 0

    // Delta of 0 is stored as -32768.
    private val packed: Array[Short] = {
      val packed = new Array[Short](size)
      var i = 1
      var last = base
      packed(0) = -32768.toShort
      while (i < size) {
        packed(i) = (data(i) - last - 32768).toShort
        last = data(i)
        i += 1
      }
      packed
    }

    override def iterator = new IntIterator {
      var index = 0
      var base: Int = DeltaEncoded16.this.base
      override def next = {
        base += packed(index).toInt + 32768
        index += 1
        base
      }
    }
  }
}
