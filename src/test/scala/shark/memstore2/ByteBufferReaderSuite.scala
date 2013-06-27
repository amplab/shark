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

  val data_size = 4 + 8 + 1 + 8 + 4 + 2 + 3 + 2*2 + 3*4 + 3*8 + 3*4 + 3*8

  def putData(buf: ByteBuffer) {
    buf.order(ByteOrder.nativeOrder())
    buf.putInt(655350)
    buf.putLong(10034534500L)
    buf.put(1.toByte)
    buf.putDouble(1.5.toDouble)
    buf.putFloat(2.5.toFloat)
    buf.putShort(2.toShort)
    buf.put(Array[Byte](15.toByte, 25.toByte, 35.toByte))
    buf.putShort(10.toShort)
    buf.putShort(1000.toShort)
    buf.putInt(655360)
    buf.putInt(23)
    buf.putInt(134)
    buf.putLong(134134234L)
    buf.putLong(-23454352346L)
    buf.putLong(3245245425L)
    buf.putFloat(1.0.toFloat)
    buf.putFloat(2.0.toFloat)
    buf.putFloat(4.0.toFloat)
    buf.putDouble(1.1)
    buf.putDouble(2.2)
    buf.putDouble(3.3)
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

    val shorts = new Array[Short](2)
    reader.getShorts(shorts, 2)
    assert(shorts(0) === 10.toShort)
    assert(shorts(1) === 1000.toShort)

    val ints = new Array[Int](3)
    reader.getInts(ints, 3)
    assert(ints(0) === 655360)
    assert(ints(1) === 23)
    assert(ints(2) === 134)

    val longs = new Array[Long](3)
    reader.getLongs(longs, 3)
    assert(longs(0) === 134134234L)
    assert(longs(1) === -23454352346L)
    assert(longs(2) === 3245245425L)

    val floats = new Array[Float](3)
    reader.getFloats(floats, 3)
    assert(floats(0) === 1.0.toFloat)
    assert(floats(1) === 2.0.toFloat)
    assert(floats(2) === 4.0.toFloat)

    val doubles = new Array[Double](3)
    reader.getDoubles(doubles, 3)
    assert(doubles(0) === 1.1)
    assert(doubles(1) === 2.2)
    assert(doubles(2) === 3.3)
  }

  def testPosition(reader: ByteBufferReader) {
    reader.position(4)
    assert(reader.getLong() === 10034534500L)
  }
}
