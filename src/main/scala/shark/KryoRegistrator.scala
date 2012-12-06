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

package shark

import java.nio.ByteBuffer
import java.util.Arrays

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.serialize.{IntSerializer, SimpleSerializer, SerializableSerializer}

import de.javakaffee.kryoserializers.ArraysAsListSerializer

import org.apache.hadoop.hive.ql.exec.persistence.{MapJoinSingleKey, MapJoinObjectKey,
  MapJoinDoubleKeys, MapJoinObjectValue}

import shark.execution.MapJoinOperator


class KryoRegistrator extends spark.KryoRegistrator {
  def registerClasses(kryo: Kryo) {

    kryo.register(classOf[execution.ReduceKey])

    // Java Arrays.asList returns an internal class that cannot be serialized
    // by default Kryo. This provides a workaround.
    kryo.register(Arrays.asList().getClass, new ArraysAsListSerializer(kryo))

    kryo.register(classOf[MapJoinSingleKey], new SerializableSerializer)
    kryo.register(classOf[MapJoinObjectKey], new SerializableSerializer)
    kryo.register(classOf[MapJoinDoubleKeys], new SerializableSerializer)
    kryo.register(classOf[MapJoinObjectValue], new SerializableSerializer)

    // This is a work-around for Kryo's byte-by-byte serialization of byte
    // arrays. Note that it does not handle nulls because null rows are already
    // encoded using Hive's SerDe.
    kryo.register(classOf[Array[Byte]], new SimpleSerializer[Array[Byte]]() {
      def write (buffer: ByteBuffer, arr: Array[Byte]) {
        IntSerializer.put(buffer, arr.length, true)
        buffer.put(arr)
      }
      def read (buffer: ByteBuffer): Array[Byte] =  {
        val len = IntSerializer.get(buffer, true)
        val arr = new Array[Byte](len)
        buffer.get(arr)
        arr
      }
    })
  }
}
