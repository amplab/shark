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

  override def getInt(): Int = _buf.getInt()

  override def getLong(): Long = _buf.getLong()

  override def getFloat(): Float = _buf.getFloat()

  override def getDouble(): Double = _buf.getDouble()

  override def position(newPosition: Int) {
    _buf.position(newPosition)
  }
}
