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
import java.io.InputStream


object LZFSerializer{
  val MIN_CHUNK_BYTES = 230 // a safe set of bytes to account for stuff added on
                            // by LZF even if there is nothing to compress
  val BLOCK_SIZE = 65536


  def encode(b: Array[Byte]): Array[Byte] = LZFEncoder.encode(b)
  def decode(b: Array[Byte]): Array[Byte] = LZFDecoder.decode(b)


  // Also advances the buffer to the point after the uncompressedBytes have been written
  def writeToBuffer(buf: ByteBuffer, numUncompressedBytes: Int, compressedBytes: Array[Byte]) {
    buf.putInt(numUncompressedBytes)
    val len = compressedBytes.size
    buf.putInt(len)
    val iter = compressedBytes.iterator
    while (iter.hasNext) {
      buf.put(iter.next)
    }
    buf
  }

  // Also advances the buffer to the point after the uncompressedBytes have been read
  def readFromBuffer(buf: ByteBufferReader): (Int, Array[Byte]) = {
    val numUncompressedBytes = buf.getInt()
    val num = buf.getInt()
    var compressedBytes = new Array[Byte](num)

    var i = 0
    while (i < num) {
      compressedBytes(i) = (buf.getByte())
      i += 1
    }
    (numUncompressedBytes, compressedBytes)
  }


}
