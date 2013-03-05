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

import java.nio.{ByteBuffer, HeapByteBuffer}

/**
 * An implementation of the ByteBufferReader using sun.misc.Unsafe. This provides very high
 * throughput read of various primitive types from a HeapByteBuffer, but can potentially
 * crash the JVM if the implementation is faulty.
 */
class UnsafeHeapByteBufferReader(buf: ByteBuffer) extends ByteBufferReader {

  if (!buf.hasArray()) {
    throw new IllegalArgumentException("buf (" + buf + ") must have a backing array. ")
  }

  private val _base_offset: Long = Unsafe.BYTE_ARRAY_BASE_OFFSET
  private var _offset: Long = _base_offset
  private var _arr: Array[Byte] = buf.array()

  override def getByte(): Byte = {
    val v = Unsafe.unsafe.getByte(_arr, _offset)
    _offset += 1
    v
  }

  override def getBytes(dst: Array[Byte], length: Int) {
    Unsafe.unsafe.copyMemory(_arr, _offset, dst, Unsafe.BYTE_ARRAY_BASE_OFFSET, length)
    _offset += length
  }

  override def getShort(): Short = {
    val v = Unsafe.unsafe.getShort(_arr, _offset)
    _offset += 2
    v
  }

  override def getInt(): Int = {
    val v = Unsafe.unsafe.getInt(_arr, _offset)
    _offset += 4
    v
  }

  override def getLong(): Long = {
    val v = Unsafe.unsafe.getLong(_arr, _offset)
    _offset += 8
    v
  }

  override def getFloat(): Float = {
    val v = Unsafe.unsafe.getFloat(_arr, _offset)
    _offset += 4
    v
  }

  override def getDouble(): Double = {
    val v = Unsafe.unsafe.getDouble(_arr, _offset)
    _offset += 8
    v
  }

  override def position(newPosition: Int) {
    _offset = _base_offset + newPosition
  }
}
