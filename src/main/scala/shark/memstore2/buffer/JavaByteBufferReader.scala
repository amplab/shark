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

import java.nio.{ByteBuffer, ByteOrder}


/**
 * An implementation of the ByteBufferReader using methods from ByteBuffer.
 */
class JavaByteBufferReader(buf: ByteBuffer) extends ByteBufferReader {

  val _buf = buf.duplicate()
  _buf.order(ByteOrder.nativeOrder())

  override def getByte(): Byte = _buf.get()

  override def getBytes(dst: Array[Byte], length: Int) {
    _buf.get(dst, 0, length)
  }

  override def getShort(): Short = _buf.getShort()

  override def getShorts(dst: Array[Short], len: Int) {
    var i = 0
    while (i < len) {
      dst(i) = _buf.getShort()
      i += 1
    }
  }

  override def getInt(): Int = _buf.getInt()

  override def getInts(dst: Array[Int], len: Int) {
    var i = 0
    while (i < len) {
      dst(i) = _buf.getInt()
      i += 1
    }
  }

  override def getLong(): Long = _buf.getLong()

  override def getLongs(dst: Array[Long], len: Int) {
    var i = 0
    while (i < len) {
      dst(i) = _buf.getLong()
      i += 1
    }
  }

  override def getFloat(): Float = _buf.getFloat()

  override def getFloats(dst: Array[Float], len: Int) {
    var i = 0
    while (i < len) {
      dst(i) = _buf.getFloat()
      i += 1
    }
  }

  override def getDouble(): Double = _buf.getDouble()

  override def getDoubles(dst: Array[Double], len: Int) {
    var i = 0
    while (i < len) {
      dst(i) = _buf.getDouble()
      i += 1
    }
  }

  override def position(newPosition: Int) {
    _buf.position(newPosition)
  }

  override def position: Int = _buf.position()

  override def duplicate(): ByteBufferReader = {
    // JavaByteBufferReader in its constructor will make a duplicate of the buffer.
    new JavaByteBufferReader(_buf)
  }

  override def printDebug() {
    val b = _buf.duplicate()
    while (b.hasRemaining()) print(b.get() + " ")
  }
}
