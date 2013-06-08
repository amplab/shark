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

import scala.collection.mutable._
import shark.memstore2.buffer.ByteBufferReader

import it.unimi.dsi.fastutil.bytes.ByteArrayList

import com.ning.compress.lzf.LZFEncoder
import com.ning.compress.lzf.LZFDecoder
import com.ning.compress.lzf.LZFInputStream
import java.nio.ByteBuffer
import java.nio.ByteOrder


// class LZFBlockSerializer{

// LZF chunk length is 64K - so to get one least one chunk in the decode always
// try to decode more than that.

//   def decodeStream(

// }

object LZFSerializer{

  def encode(b: Array[Byte]) = LZFEncoder.encode(b)
  def decode(b: Array[Byte]) = LZFDecoder.decode(b)

  // Also advances the buffer to the point after the uncompressedBytes have been written
  def writeToBuffer(buf: ByteBuffer, compressedBytes: Array[Byte]) {
    val len = compressedBytes.size
    val iter = compressedBytes.iterator
    buf.putInt(len)
    while (iter.hasNext) {
      buf.put(iter.next)
    }
    buf
  }

  // Also advances the buffer to the point after the uncompressedBytes have been read
  def readFromBuffer(buf: ByteBufferReader): Array[Byte] = {
    val num = buf.getInt()
    var compressedBytes = new Array[Byte](num)

    var i = 0
    while (i < num) {
      compressedBytes(i) = (buf.getByte())
      i += 1
    }
    compressedBytes
  }

}
