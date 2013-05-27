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
import scala.collection.mutable.ListBuffer

import shark.memstore2.buffer.ByteBufferReader
import shark.memstore2.buffer.Unsafe

/* 
 * Provide a simple serializer to store dictionaries for compression.
 * These dictionaries will be serialized in the ByteBuffer so that they can go
 * between the column builder and column iterator.
 * 
 * 
 */

class Dictionary(uniqueInts: List[Int]){

  // return the Int associated with Byte
  def get(b:Byte) = {
    // Convert byte to index
    // -128, 128 ===> 0, 256
    var idx: Int = b
    if(b < 0)
      idx = b + 256

    if (idx < 0 || size <= idx)
      throw new IndexOutOfBoundsException("Index " + idx)

    uniqueInts(idx)
  }

  // return the Byte representation for Int value
  def getByte(i: Int): Byte = 
    uniqueInts.indexOf(i).toByte

  def sizeinbits = {
    val ret = 32 * (1 + uniqueInts.size) 
    // println("Need " + ret + " Bytes to store dictionary in memory")
    ret
  }

  def size = uniqueInts.size
}


object DictionarySerializer{

  // Append the serialized bytes of the Dictionary into the ByteBuffer.
  def writeToBuffer(buf: ByteBuffer, bitmap: Dictionary) {
    // compression size in Bytes
    // compression dict Bytes
    buf.putInt(bitmap.sizeinbits/8)
    var pos: Int = 0
    while (pos < bitmap.size) {
      // println("pos was " + pos)
      buf.putInt(bitmap.get(pos.toByte))
      pos += 1
    }
    buf
  }

  // Create an Dictionary from the byte buffer.
  def readFromBuffer(bufReader: ByteBufferReader): Dictionary = {

    val bufferLengthInBytes = bufReader.getInt()
    
    val uniqueCount = (bufferLengthInBytes - 4)/4 // 4 Bytes used to store length
    var uniqueInts = new ListBuffer[Int]()
    var i = 0
    while (i < uniqueCount) {
      uniqueInts += bufReader.getInt()
      i += 1
    }

    val bitmap = new Dictionary(uniqueInts.toList)
    bitmap
  }
}
