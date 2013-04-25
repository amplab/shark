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

import java.io.{InputStream, OutputStream}
import java.nio.ByteBuffer

import org.apache.hadoop.io.BytesWritable

import shark.execution.ReduceKey

import spark.serializer.{DeserializationStream, Serializer, SerializerInstance, SerializationStream}


class ShuffleSerializer extends Serializer {
  override def newInstance(): SerializerInstance = new ShuffleSerializerInstance
}


class ShuffleSerializerInstance extends SerializerInstance {

  override def serialize[T](t: T): ByteBuffer = throw new UnsupportedOperationException

  override def deserialize[T](bytes: ByteBuffer): T = throw new UnsupportedOperationException

  override def deserialize[T](bytes: ByteBuffer, loader: ClassLoader): T =
    throw new UnsupportedOperationException

  override def serializeStream(s: OutputStream): SerializationStream = {
    new ShuffleSerializationStream(s)
  }

  override def deserializeStream(s: InputStream): DeserializationStream = {
    new ShuffleDeserializationStream(s)
  }
}


class ShuffleSerializationStream(stream: OutputStream) extends SerializationStream {

  override def writeObject[T](t: T): SerializationStream = {
    val pair = t.asInstanceOf[(ReduceKey, BytesWritable)]
    val keyLen = pair._1.getLength()
    val valueLen = pair._2.getLength()
    writeInt(keyLen)
    writeInt(valueLen)
    stream.write(pair._1.getBytes(), 0, keyLen)
    stream.write(pair._2.getBytes(), 0, valueLen)
    this
  }

  override def flush(): Unit = stream.flush()

  override def close(): Unit = {}

  def writeInt(value: Int) {
    stream.write(value >> 24)
    stream.write(value >> 16)
    stream.write(value >> 8)
    stream.write(value)
  }
}


class ShuffleDeserializationStream(stream: InputStream) extends DeserializationStream {

  //val keyBytes = new BytesWritable
  //val reduceKey = new ReduceKey(keyBytes)
  //val valueBytes = new BytesWritable

  override def readObject[T](): T = {
    val keyLen = readInt()
    if (keyLen < 0) {
      throw new java.io.EOFException
    }
    val valueLen = readInt()
    // keyBytes.setSize(keyLen)
    // stream.read(keyBytes.getBytes(), 0, keyLen)
    // valueBytes.setSize(valueLen)
    // stream.read(valueBytes.getBytes(), 0, valueLen)
    // (new ReduceKey(keyBytes), valueBytes).asInstanceOf[T]
    val keyBytes = new BytesWritable(new Array[Byte](keyLen))
    stream.read(keyBytes.getBytes(), 0, keyLen)
    val valueBytes = new BytesWritable(new Array[Byte](valueLen))
    stream.read(valueBytes.getBytes(), 0, valueLen)
    (new ReduceKey(keyBytes), valueBytes).asInstanceOf[T]
  }

  override def close() { }

  def readInt(): Int = {
    stream.read() << 24 | stream.read() << 16 | stream.read() << 8 | stream.read()
  }
}
