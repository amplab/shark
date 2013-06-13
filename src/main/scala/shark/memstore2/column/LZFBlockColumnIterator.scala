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
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.io.InputStream
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream

import com.ning.compress.lzf.LZFInputStream
import com.ning.compress.lzf.LZFOutputStream
import com.ning.compress.lzf.LZFEncoder
import com.ning.compress.lzf.LZFDecoder

import shark.LogHelper

/**
 * A wrapper that uses LZ compression. Decompresses in blocks (or chunks) so
 * that large amounts of scratch space will not required for large columns. LZF
 * is a chunk-based algorithm in any case.
 */


class LZFBlockColumnIterator[T <: ColumnIterator](
  baseIterCls: Class[T], bytes: ByteBufferReader)
    extends ColumnIterator {

  var initialized = false
  var (numUncompressedBytes, compressedArr) = LZFSerializer.readFromBuffer(bytes)
  // use a safe larger size
  val minChunkSize = math.max(LZFSerializer.MIN_CHUNK_BYTES, 2*LZFSerializer.BLOCK_SIZE)
  val uncompressedArr = new Array[Byte](minChunkSize)

  val bis: ByteArrayInputStream = new ByteArrayInputStream(compressedArr)
  val is: InputStream = new LZFInputStream(bis)

  val uncompressedBB = ByteBuffer.allocate(numUncompressedBytes)
  // logDebug("numUncompressedBytes " + numUncompressedBytes)
  uncompressedBB.order(ByteOrder.nativeOrder())

  val baseIter: T = {
    val ctor = baseIterCls.getConstructor(classOf[ByteBufferReader])
    val uncompressedBBR = ByteBufferReader.createUnsafeReader(uncompressedBB)
    ctor.newInstance(uncompressedBBR).asInstanceOf[T]
  }


  var rowCount = 0
  override def next = {
    initialized = true
    (rowCount % LZFSerializer.BLOCK_SIZE) match {
      case 0 => {     // may need new chunk - get that and populate BB
        val num = is.read(uncompressedArr)
        if(num > 0) {
          uncompressedBB.put(uncompressedArr, 0, num)
        } else {
          null // no elements
        }
      }
      case _ => {
        // do nothing - chunk populated
      }
    }

    rowCount += 1
    baseIter.next()
  }

  // Semantics are to not change state - read-only
  override def current: Object = {
    if (!initialized) {
      throw new RuntimeException("LZFBlockColumnIterator next() should be called first")
    } else {
      baseIter.current
    }
  }
}
