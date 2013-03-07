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

package shark.memstore2.buffer

import java.nio.ByteBuffer


trait ByteBufferReader {

  def getByte(): Byte

  def getBytes(dst: Array[Byte], len: Int)

  def getShort(): Short

  def getShorts(dst: Array[Short], len: Int)

  def getInt(): Int

  def getInts(dst: Array[Int], len: Int)

  def getLong(): Long

  def getLongs(dst: Array[Long], len: Int)

  def getFloat(): Float

  def getFloats(dst: Array[Float], len: Int)

  def getDouble(): Double

  def getDoubles(dst: Array[Double], len: Int)

  def position(newPosition: Int)
}


object ByteBufferReader {

  def createUnsafeReader(buf: ByteBuffer): ByteBufferReader = {
    if (buf.hasArray()) {
      Console.flush()
      new UnsafeHeapByteBufferReader(buf)
    } else {
      Console.flush()
      new UnsafeDirectByteBufferReader(buf)
    }
  }
}
