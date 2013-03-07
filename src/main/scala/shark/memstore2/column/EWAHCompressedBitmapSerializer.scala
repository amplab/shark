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

package javaewah
// Put it here so we can access package level visible variables in EWAHCompressedBitmap.
// This is a bit crazy but gets us much higher performance.

import java.nio.ByteBuffer

import shark.memstore2.buffer.ByteBufferReader
import shark.memstore2.buffer.Unsafe


object EWAHCompressedBitmapSerializer {

  def serializeIntoBuffer(buf: ByteBuffer, bitmap: EWAHCompressedBitmap) {
    buf.putInt(bitmap.sizeinbits)
    buf.putInt(bitmap.actualsizeinwords)
    buf.putInt(bitmap.buffer.length)
    var i = 0
    while (i < bitmap.actualsizeinwords) {
      buf.putLong(bitmap.buffer(i))
      i += 1
    }
    buf.putInt(bitmap.rlw.position)
  }

  def deserializeFromBuffer(buf: ByteBufferReader): EWAHCompressedBitmap = {
    val bitmap = Unsafe.unsafe.allocateInstance(
      classOf[EWAHCompressedBitmap]).asInstanceOf[EWAHCompressedBitmap]

    bitmap.sizeinbits = buf.getInt()
    bitmap.actualsizeinwords = buf.getInt()
    val bufferLength = buf.getInt()
    bitmap.buffer = new Array[Long](bufferLength)
    buf.getLongs(bitmap.buffer, bufferLength)
    bitmap.rlw = new RunningLengthWord(bitmap.buffer, buf.getInt())
    bitmap
  }
}

