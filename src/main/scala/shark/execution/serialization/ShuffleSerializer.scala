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

import org.apache.spark.SparkConf
import org.apache.spark.serializer.DeserializationStream
import org.apache.spark.serializer.{SerializationStream, Serializer, SerializerInstance}

import shark.execution.{ReduceKey, ReduceKeyReduceSide}

/**
 * A serializer for Shark/Hive-specific serialization used in Spark shuffle. Since this is only
 * used in shuffle operations, only serializeStream and deserializeStream are implemented.
 *
 * The serialization process is very simple:
 * - Shark operators use Hive serializers to serialize the data structures into byte arrays
 *   (wrapped in BytesWritable object).
 * - Shark operators wrap each key (BytesWritable) in a ReduceKeyMapSide object. The values remain
 *   unchanged as BytesWritable.
 * - ShuffleSerializationStream simply flushes the underlying byte arrays for key/value into the
 *   serialization stream. The length is prepended before the byte array so the deserializer knows
 *   how many bytes to read.
 *
 * The deserialization process simply reverses the above, with a few caveats:
 * - The data type for the keys becomes ReduceKeyReduceSide, wrapping around a byte array (rather
 *   than a BytesWritable).
 * - The data type for the values becomes a byte array, rather than a BytesWritable.
 * The reason is that during aggregations and joins (post shuffle), the key-value pairs are inserted
 * into a hash table. We want to reduce the size of the hash table. Having the BytesWritable wrapper
 * would increase the size of the hash table by another 16 bytes per key-value pair.
 */
class ShuffleSerializer(conf: SparkConf) extends Serializer {

  // A no-arg constructor since conf is not needed in this serializer.
  def this() = this(null)

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
    // On the write-side, the ReduceKey should be of type ReduceKeyMapSide.
    val (key, value) = t.asInstanceOf[(ReduceKey, BytesWritable)]
    writeUnsignedVarInt(key.length)
    writeUnsignedVarInt(value.getLength)
    stream.write(key.byteArray, 0, key.length)
    stream.write(value.getBytes(), 0, value.getLength)
    this
  }

  override def flush() {
    stream.flush()
  }

  override def close() {
    stream.close()
  }

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

  override def readObject[T](): T = {
    // Return type is (ReduceKeyReduceSide, Array[Byte])
    val keyLen = readUnsignedVarInt()
    if (keyLen < 0) {
      throw new java.io.EOFException
    }
    val valueLen = readUnsignedVarInt()
    val keyByteArray = new Array[Byte](keyLen)
    readFully(stream, keyByteArray, keyLen)
    val reduceKey = new ReduceKeyReduceSide(keyByteArray)

    if (valueLen > 0) {
      val valueByteArray = new Array[Byte](valueLen)
      readFully(stream, valueByteArray, valueLen)
      (reduceKey, valueByteArray).asInstanceOf[T]
    } else {
      (reduceKey, ShuffleDeserializationStream.EMPTY_BYTES).asInstanceOf[T]
    }
  }

  def readFully(stream: InputStream, bytes: Array[Byte], length: Int) {
    var read = 0
    while (read < length) {
      read += stream.read(bytes, read, length - read)
    }
  }

  override def close() {
    stream.close()
  }

  def readUnsignedVarInt(): Int = {
    var value: Int = 0
    var i: Int = 0
    def readOrThrow(): Int = {
      val in = stream.read()
      if (in < 0) throw new java.io.EOFException
      in & 0xFF
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


object ShuffleDeserializationStream {
  val EMPTY_BYTES = Array.empty[Byte]
}
