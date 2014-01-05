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

import java.nio.ByteBuffer

import org.scalatest.FunSuite

import org.apache.spark.SparkConf
import org.apache.spark.serializer.{JavaSerializer, KryoSerializer}


class TablePartitionSuite extends FunSuite {

  test("serialize TablePartition backed by non-direct ByteBuffer using Java") {
    val col1 = Array[Byte](0, 1, 2)
    val col2 = Array[Byte](1, 2, 3)
    val tp = new TablePartition(3, Array(ByteBuffer.wrap(col1), ByteBuffer.wrap(col2)))

    val ser = new JavaSerializer(new SparkConf(false))
    val bytes = ser.newInstance().serialize(tp)
    val tp1 = ser.newInstance().deserialize[TablePartition](bytes)
    assert(tp1.numRows === 3)
    assert(tp1.columns(0).remaining() == 3)
    assert(tp1.columns(0).get() == 0)
    assert(tp1.columns(0).get() == 1)
    assert(tp1.columns(0).get() == 2)
    assert(tp1.columns(1).remaining() == 3)
    assert(tp1.columns(1).get() == 1)
    assert(tp1.columns(1).get() == 2)
    assert(tp1.columns(1).get() == 3)
  }

  test("serialize TablePartition backed by direct ByteBuffer using Java") {
    val col1 = ByteBuffer.allocateDirect(3)
    col1.put(0.toByte)
    col1.put(1.toByte)
    col1.put(2.toByte)
    col1.rewind()
    val col2 = ByteBuffer.allocateDirect(3)
    col2.put(1.toByte)
    col2.put(2.toByte)
    col2.put(3.toByte)
    col2.rewind()
    val tp = new TablePartition(3, Array(col1, col2))

    val ser = new JavaSerializer(new SparkConf(false))
    val bytes = ser.newInstance().serialize(tp)
    val tp1 = ser.newInstance().deserialize[TablePartition](bytes)
    assert(tp1.numRows === 3)
    assert(tp1.columns(0).remaining() == 3)
    assert(tp1.columns(0).get() == 0)
    assert(tp1.columns(0).get() == 1)
    assert(tp1.columns(0).get() == 2)
    assert(tp1.columns(1).remaining() == 3)
    assert(tp1.columns(1).get() == 1)
    assert(tp1.columns(1).get() == 2)
    assert(tp1.columns(1).get() == 3)
  }

  test("serialize TablePartition backed by non-direct ByteBuffer using Kryo") {
    val col1 = Array[Byte](0, 1, 2)
    val col2 = Array[Byte](1, 2, 3)
    val tp = new TablePartition(3, Array(ByteBuffer.wrap(col1), ByteBuffer.wrap(col2)))

    val ser = new KryoSerializer(new SparkConf(false))
    val bytes = ser.newInstance().serialize(tp)
    val tp1 = ser.newInstance().deserialize[TablePartition](bytes)
    assert(tp1.numRows === 3)
    assert(tp1.columns(0).remaining() == 3)
    assert(tp1.columns(0).get() == 0)
    assert(tp1.columns(0).get() == 1)
    assert(tp1.columns(0).get() == 2)
    assert(tp1.columns(1).remaining() == 3)
    assert(tp1.columns(1).get() == 1)
    assert(tp1.columns(1).get() == 2)
    assert(tp1.columns(1).get() == 3)
  }

  test("serialize TablePartition backed by direct ByteBuffer using Kryo") {
    val col1 = ByteBuffer.allocateDirect(3)
    col1.put(0.toByte)
    col1.put(1.toByte)
    col1.put(2.toByte)
    col1.rewind()
    val col2 = ByteBuffer.allocateDirect(3)
    col2.put(1.toByte)
    col2.put(2.toByte)
    col2.put(3.toByte)
    col2.rewind()
    val tp = new TablePartition(3, Array(col1, col2))

    val ser = new KryoSerializer(new SparkConf(false))
    val bytes = ser.newInstance().serialize(tp)
    val tp1 = ser.newInstance().deserialize[TablePartition](bytes)
    assert(tp1.numRows === 3)
    assert(tp1.columns(0).remaining() == 3)
    assert(tp1.columns(0).get() == 0)
    assert(tp1.columns(0).get() == 1)
    assert(tp1.columns(0).get() == 2)
    assert(tp1.columns(1).remaining() == 3)
    assert(tp1.columns(1).get() == 1)
    assert(tp1.columns(1).get() == 2)
    assert(tp1.columns(1).get() == 3)
  }
}
