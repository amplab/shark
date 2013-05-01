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
    writeUnsignedVarInt(keyLen)
    writeUnsignedVarInt(valueLen)
    stream.write(pair._1.getBytes(), 0, keyLen)
    stream.write(pair._2.getBytes(), 0, valueLen)
    this
  }

  override def flush(): Unit = stream.flush()

  override def close(): Unit = {}

  def writeUnsignedVarInt(value: Int) {
    var v = value
    while ((v & 0xFFFFFF80) != 0L) {
      stream.write((v & 0x7F) | 0x80)
      v = v >>> 7
    }
    stream.write(v & 0x7F)
  }
}


class ShuffleDeserializationStream(stream: InputStream) extends DeserializationStream {

  //val keyBytes = new BytesWritable
  //val reduceKey = new ReduceKey(keyBytes)
  //val valueBytes = new BytesWritable

  override def readObject[T](): T = {
    val keyLen = readUnsignedVarInt()
    if (keyLen < 0) {
      throw new java.io.EOFException
    }
    val valueLen = readUnsignedVarInt()
    val keyBytes = new BytesWritable(new Array[Byte](keyLen))
    stream.read(keyBytes.getBytes(), 0, keyLen)
    val valueBytes = new BytesWritable(new Array[Byte](valueLen))
    stream.read(valueBytes.getBytes(), 0, valueLen)
    (new ReduceKey(keyBytes), valueBytes).asInstanceOf[T]
  }

  override def close() { }

  def readUnsignedVarInt(): Int = {
    var value: Int = 0
    var i: Int = 0
    def readOrThrow(): Int = {
      val in = stream.read()
      if (in < 0) throw new java.io.EOFException
      return in & 0xFF
    }
    var b: Int = readOrThrow()
    while ((b & 0x80) != 0) {
      value |= (b & 0x7F) << i
      i += 7
      if (i > 35) throw new IllegalArgumentException("Variable length quantity is too long")
      b = readOrThrow()
    }
    value | (b << i)
  }
}
