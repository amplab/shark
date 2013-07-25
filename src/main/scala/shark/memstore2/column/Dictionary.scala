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
import scala.collection.mutable.{ListBuffer}
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.IntWritable

import shark.memstore2.buffer.ByteBufferReader

/** Provide a simple serializer to store dictionaries for compression.  These
 * dictionaries will be serialized in the ByteBuffer so that they can go between
 * the column builder and column iterator.
 * 
 */
trait Dictionary[@specialized(Int) T]{
  var uniques: Array[T]
  def initialize(u: Array[T]): Unit
  def get(b: Byte): T
  def getWritable(b: Byte): Object
  def getByte(t: T): Byte
  def sizeInBits: Int
  def size: Int = uniques.size
}


/** Int implementation. Saves space by using 1 byte per Int
  * instead of 4.
  */
class IntDictionary extends Dictionary[Int]{
  var uniques = new Array[Int](0)
  private var writable = new IntWritable

  override def initialize(u: Array[Int]) = {
    uniques = new Array[Int](u.size)
    u.copyToArray(uniques)
  }

  // return the Int associated with Byte
  override def get(b:Byte): Int = {
    // Convert byte to index
    // -128, 128 ===> 0, 256
    var idx: Int = b
    if(b < 0) {
      idx = b + 256
    }

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

  override def sizeInBits = {
    val ret = 32 * (1 + uniques.size) 
    // println("Need " + ret + " Bytes to store dictionary in memory")
    ret
  }
}

// class StringDictionary extends Dictionary[Text]{

// }

/** Helper to share code betwen the Iterator and various Builders.
  */
object DictionarySerializer{

  // Append the serialized bytes of the Dictionary into the ByteBuffer.
  def writeToBuffer(buf: ByteBuffer, bitmap: IntDictionary) {
    // compression size in Bytes
    // compression dict Bytes
    buf.putInt(bitmap.sizeInBits/8)
    var pos: Int = 0
    while (pos < bitmap.size) {
      // println("writetobuffer: pos was " + pos + 
      //   " sizeInBits/8 " + bitmap.sizeInBits/8 + 
      //   " bitmap.size " + bitmap.size)
      buf.putInt(bitmap.get(pos.toByte))
      pos += 1
    }
    buf
  }

  // Create a Dictionary from the byte buffer.
  def readFromBuffer(bufReader: ByteBufferReader): Dictionary[Int] = {
    // println("bufReader.position before dict  " + bufReader.position)

    val bufferLengthInBytes = bufReader.getInt()
    
    val uniqueCount = (bufferLengthInBytes - 4)/4 // 4 Bytes used to store length
    var uniques = new Array[Int](uniqueCount)
    bufReader.getInts(uniques, uniqueCount)

    val dict = new IntDictionary
    // println("bufferLengthInBytes " + bufferLengthInBytes + " readFromBuffer: uniques.size " + uniques.size)
    dict.initialize(uniques)
    dict
  }
}
