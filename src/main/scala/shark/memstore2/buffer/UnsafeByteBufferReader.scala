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


/**
 * An implementation of the ByteBufferReader using sun.misc.Unsafe. This provides very high
 * throughput read of various primitive types from a ByteBuffer, but can potentially
 * crash the JVM if the implementation is faulty.
 */
class UnsafeByteBufferReader(buf: java.nio.ByteBuffer) extends ByteBufferReader {

  private var _offset: Long = UnsafeByteBufferReader.findAddress(buf)
  private var _end: Long = _offset + buf.capacity()

  override def getByte(): Byte = {
    val v = UnsafeByteBufferReader.unsafe.getByte(_offset)
    _offset += 1
    v
  }

  override def getShort(): Short = {
    val v = UnsafeByteBufferReader.unsafe.getShort(_offset)
    _offset += 2
    v
  }

  override def getInt(): Int = {
    val v = UnsafeByteBufferReader.unsafe.getInt(_offset)
    _offset += 4
    v
  }

  override def getLong(): Long = {
    val v = UnsafeByteBufferReader.unsafe.getLong(_offset)
    _offset += 1
    v
  }

  override def getFloat(): Float = {
    val v = UnsafeByteBufferReader.unsafe.getFloat(_offset)
    _offset += 1
    v
  }

  override def getDouble(): Double = {
    val v = UnsafeByteBufferReader.unsafe.getDouble(_offset)
    _offset += 1
    v
  }
}


object UnsafeByteBufferReader {

  val unsafe: sun.misc.Unsafe = {
    val unsafeField = classOf[sun.misc.Unsafe].getDeclaredField("theUnsafe")
    unsafeField.setAccessible(true)
    unsafeField.get(null).asInstanceOf[sun.misc.Unsafe]
  }

  def findAddress(buffer: java.nio.ByteBuffer): Long = {
    if (buffer.hasArray) {
      val baseOffset: Long = unsafe.arrayBaseOffset(classOf[Array[Byte]])

      unsafe.addressSize() match {
        case 4 => unsafe.getInt(buffer.array(), baseOffset)
        case 8 => unsafe.getLong(buffer.array(), baseOffset)
        case _ => throw new Error("unsupported address size: " + unsafe.addressSize())
      }
    } else {
      val addressField = classOf[java.nio.Buffer].getDeclaredField("address")
      addressField.setAccessible(true)
      addressField.get(buffer).asInstanceOf[Long]
    }
  }
}
