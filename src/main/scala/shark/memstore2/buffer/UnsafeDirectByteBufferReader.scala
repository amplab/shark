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
 * throughput read of various primitive types from a ByteBuffer, but can potentially
 * crash the JVM if the implementation is faulty.
 */
class UnsafeDirectByteBufferReader(buf: java.nio.ByteBuffer) extends ByteBufferReader {

  private val _base_offset = getMemoryAddress(buf)
  private var _offset: Long = _base_offset

  override def getByte(): Byte = {
    val v = Unsafe.unsafe.getByte(_offset)
    _offset += 1
    v
  }

  override def getBytes(dst: Array[Byte], length: Int) {
    Unsafe.unsafe.copyMemory(null, _offset, dst, Unsafe.BYTE_ARRAY_BASE_OFFSET, length)
    _offset += length
  }

  override def getShort(): Short = {
    val v = Unsafe.unsafe.getShort(_offset)
    _offset += 2
    v
  }

  override def getShorts(dst: Array[Short], length: Int) {
    Unsafe.unsafe.copyMemory(null, _offset, dst, Unsafe.BYTE_ARRAY_BASE_OFFSET, length * 2)
    _offset += length * 2
  }

  override def getInt(): Int = {
    val v = Unsafe.unsafe.getInt(_offset)
    _offset += 4
    v
  }

  override def getInts(dst: Array[Int], length: Int) {
    Unsafe.unsafe.copyMemory(null, _offset, dst, Unsafe.BYTE_ARRAY_BASE_OFFSET, length * 4)
    _offset += length * 4
  }

  override def getLong(): Long = {
    val v = Unsafe.unsafe.getLong(_offset)
    _offset += 8
    v
  }

  override def getLongs(dst: Array[Long], length: Int) {
    Unsafe.unsafe.copyMemory(null, _offset, dst, Unsafe.BYTE_ARRAY_BASE_OFFSET, length * 8)
    _offset += length * 8
  }

  override def getFloat(): Float = {
    val v = Unsafe.unsafe.getFloat(_offset)
    _offset += 4
    v
  }

  override def getFloats(dst: Array[Float], length: Int) {
    Unsafe.unsafe.copyMemory(null, _offset, dst, Unsafe.BYTE_ARRAY_BASE_OFFSET, length * 4)
    _offset += length * 4
  }

  override def getDouble(): Double = {
    val v = Unsafe.unsafe.getDouble(_offset)
    _offset += 8
    v
  }

  override def getDoubles(dst: Array[Double], length: Int) {
    Unsafe.unsafe.copyMemory(null, _offset, dst, Unsafe.BYTE_ARRAY_BASE_OFFSET, length * 8)
    _offset += length * 8
  }

  override def position(newPosition: Int) {
    _offset = _base_offset + newPosition
  }

  private def getMemoryAddress(buffer: java.nio.ByteBuffer): Long = {
    val addressField = classOf[java.nio.Buffer].getDeclaredField("address")
    addressField.setAccessible(true)
    addressField.get(buffer).asInstanceOf[Long]
  }
}
