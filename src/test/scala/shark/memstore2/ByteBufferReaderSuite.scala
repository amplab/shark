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

package shark.memstore2

import java.nio.{ByteBuffer, ByteOrder}
import org.scalatest.FunSuite
import shark.memstore2.buffer._


class ByteBufferReaderSuite extends FunSuite {

  val data_size = 4 + 8 + 1 + 8 + 4 + 2 + 3

  def putData(buf: ByteBuffer) {
    buf.order(ByteOrder.nativeOrder())
    buf.putInt(655350)
    buf.putLong(10034534500L)
    buf.put(1.toByte)
    buf.putDouble(1.5.toDouble)
    buf.putFloat(2.5.toFloat)
    buf.putShort(2.toShort)
    buf.put(Array[Byte](15.toByte, 25.toByte, 35.toByte))
    buf.rewind()
  }

  def testData(reader: ByteBufferReader) {
    assert(reader.getInt() === 655350)
    assert(reader.getLong() === 10034534500L)
    assert(reader.getByte() === 1.toByte)
    assert(reader.getDouble() === 1.5)
    assert(reader.getFloat() === 2.5)
    assert(reader.getShort() === 2.toShort)

    val bytes = new Array[Byte](3)
    reader.getBytes(bytes, 3)
    assert(bytes(0) === 15.toByte)
    assert(bytes(1) === 25.toByte)
    assert(bytes(2) === 35.toByte)
  }

  def testPosition(reader: ByteBufferReader) {
    reader.position(4)
    assert(reader.getLong() === 10034534500L)
  }

  test("JavaByteBufferReader") {
    val buf = ByteBuffer.allocate(data_size)
    putData(buf)
    val reader = new JavaByteBufferReader(buf)
    testData(reader)
    testPosition(reader)
  }

  test("UnsafeDirectByteBufferReader") {
    val buf = ByteBuffer.allocateDirect(data_size)
    putData(buf)
    val reader = ByteBufferReader.createUnsafeReader(buf)
    testData(reader)
    testPosition(reader)
  }

  test("UnsafeHeapByteBufferReader") {
    val buf = ByteBuffer.allocate(data_size)
    putData(buf)
    val reader = ByteBufferReader.createUnsafeReader(buf)
    testData(reader)
    testPosition(reader)
  }
}
