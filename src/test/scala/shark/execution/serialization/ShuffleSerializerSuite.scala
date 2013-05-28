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

package shark.execution.serialization

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import com.ning.compress.lzf.{LZFInputStream, LZFOutputStream}

import org.apache.hadoop.io.BytesWritable
import org.scalatest.FunSuite
import org.scalatest.matchers.ShouldMatchers

import shark.execution.{ReduceKey, ReduceKeyMapSide, ReduceKeyReduceSide}


class ShuffleSerializerSuite extends FunSuite with ShouldMatchers {
  test("Encoding and decoding variable ints") {
    val check = List[Int](0, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000)

    val bos = new ByteArrayOutputStream()
    val ser = new ShuffleSerializer()
    val serOutStream = {
      ser.newInstance().serializeStream(bos).asInstanceOf[ShuffleSerializationStream]
    }
    for (i <- check) {
      serOutStream.writeUnsignedVarInt(i)
    }
    serOutStream.close();

    val bis = new ByteArrayInputStream(bos.toByteArray)
    val serInStream = {
      ser.newInstance().deserializeStream(bis).asInstanceOf[ShuffleDeserializationStream]
    }
    for (in <- check) {
      val out: Int = serInStream.readUnsignedVarInt()
      assert(out == in, "Encoded: " + in + " did not match decoded: " + out)
    }
  }

  test("Serializing and deserializing from a stream") {
    val NUM_ITEMS = 5000
    val KEY_SIZE = 1000
    val VALUE_SIZE = 1000

    val initialItems: Array[(ReduceKey, BytesWritable)] = Array.fill(NUM_ITEMS) {
      val rkBytes = (1 to KEY_SIZE).map(_.toByte).toArray
      val valueBytes = (1 to VALUE_SIZE).map(_.toByte).toArray
      val rk = new ReduceKeyMapSide(new BytesWritable(rkBytes))
      val value = new BytesWritable(valueBytes)
      (rk, value)
    }

    val bos = new ByteArrayOutputStream()
    val ser = new ShuffleSerializer()
    val serStream = ser.newInstance().serializeStream(bos)
    initialItems.map(serStream.writeObject(_))
    val bis = new ByteArrayInputStream(bos.toByteArray)
    val serInStream = ser.newInstance().deserializeStream(bis)

    initialItems.foreach { expected: (ReduceKey, BytesWritable) =>
      val output: (ReduceKey, Array[Byte]) = serInStream.readObject()
      (expected._1) should equal (output._1)
      (expected._2.getBytes) should equal (output._2)
    }
  }

  test("Serializing and deserializing from a stream (with compression)") {
    val NUM_ITEMS = 1000
    val KEY_SIZE = 1000
    val VALUE_SIZE = 1000

    val initialItems = Array.fill(NUM_ITEMS) {
      val rkBytes = (1 to KEY_SIZE).map(_.toByte).toArray
      val valueBytes = (1 to VALUE_SIZE).map(_.toByte).toArray
      val rk = new ReduceKeyMapSide(new BytesWritable(rkBytes))
      val value = new BytesWritable(valueBytes)
      (rk, value)
    }

    val bos = new ByteArrayOutputStream()
    val cBos = new LZFOutputStream(bos).setFinishBlockOnFlush(true)
    val ser = new ShuffleSerializer()
    val serStream = ser.newInstance().serializeStream(cBos)
    initialItems.map(serStream.writeObject(_))
    serStream.close()
    val array = bos.toByteArray
    val bis = new ByteArrayInputStream(array)
    val cBis = new LZFInputStream(bis)
    val serInStream = ser.newInstance().deserializeStream(cBis)

    initialItems.foreach { expected: (ReduceKey, BytesWritable) =>
      val output: (ReduceKey, Array[Byte]) = serInStream.readObject()
      (expected._1) should equal (output._1)
      (expected._2.getBytes) should equal (output._2)
    }
  }
}
