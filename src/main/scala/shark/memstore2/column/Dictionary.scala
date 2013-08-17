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
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.IntWritable

import shark.memstore2.buffer.ByteBufferReader

/** Provide a simple serializer to store dictionaries for compression.  These
 * dictionaries will be serialized in the ByteBuffer so that they can go between
 * the column builder and column iterator.
 * 
 */
trait Dictionary[@specialized(Int) T] {
  def get(b: Byte): T
  def getWritable(b: Byte): Object
  def getByte(t: T): Byte
  /* Number of bytes needed to store Dictionary */
  def sizeInBytes: Int
  /* Number of elements in Dictionary */
  def size: Int
}


/** Int implementation. Saves space by using 1 byte per Int
  * instead of 4.
  */
class IntDictionary(uniques: Array[Int]) extends Dictionary[Int] {
  require(uniques.size < DictionarySerializer.MAX_DICT_UNIQUE_VALUES,
    "Too many unique values to build an IntDictionary " + uniques.size)

  private var writable = new IntWritable

  override def size = uniques.size

  // return the Int associated with Byte
  override def get(b:Byte): Int = {
    // Convert byte to index
    // -128, 128 ===> 0, 256
    val idx = if (b < 0) (b + 256) else b

    if (idx < 0 || size <= idx) {
      throw new IndexOutOfBoundsException("Index " + idx + " Size " + size)
    }

    uniques(idx)
  }

  override def getWritable(b: Byte): Object = {
    writable.set(get(b))
    writable
  }

  // return the Byte representation for Int value
  override def getByte(i: Int): Byte = 
    uniques.indexOf(i).toByte

  override def sizeInBytes = {
    val ret = 4 * (1 + uniques.size) 
    // println("Need " + ret + " Bytes to store dictionary in memory")
    ret
  }
}

// class StringDictionary extends Dictionary[Text]{

// }

/** Helper to share code betwen the Iterator and various Builders.
  */
object DictionarySerializer {
  // Dictionaries can only work when number of unique values is lower than this. Values should fit
  // in Bytes.
  val MAX_DICT_UNIQUE_VALUES = 256 // 2 ** 8 - storable in 8 bits or 1 Byte
}

object IntDictionarySerializer {
  // Append the serialized bytes of the Dictionary into the ByteBuffer.
  def writeToBuffer(buf: ByteBuffer, dict: IntDictionary) {
    // compression size in Bytes
    // compression dict Bytes
    buf.putInt(dict.sizeInBytes)
    var pos: Int = 0
    while (pos < dict.size) {
      // println("writetobuffer: pos was " + pos + 
      //   " sizeInBytes " + bitmap.sizeInBytes + 
      //   " bitmap.size " + bitmap.size)
      buf.putInt(dict.get(pos.toByte))
      pos += 1
    }
    buf
  }

  // Create a Dictionary from the byte buffer.
  def readFromBuffer(bufReader: ByteBufferReader): IntDictionary = {
    val bufferLengthInBytes = bufReader.getInt()
    val uniqueCount = (bufferLengthInBytes - 4)/4 // 4 Bytes used to store length
    var uniques = new Array[Int](uniqueCount)
    bufReader.getInts(uniques, uniqueCount)

    // println("bufferLengthInBytes " + bufferLengthInBytes + " readFromBuffer: uniques.size " + uniques.size)
    new IntDictionary(uniques)
  }
}
