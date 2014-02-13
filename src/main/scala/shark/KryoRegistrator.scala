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

import java.io.{DataInputStream, DataOutputStream}
import java.util.Arrays
import com.esotericsoftware.kryo.{Kryo, Serializer => KSerializer}
import com.esotericsoftware.kryo.io.{Input => KryoInput, Output => KryoOutput}
import com.esotericsoftware.kryo.serializers.{JavaSerializer => KryoJavaSerializer}
import org.apache.hadoop.io.Writable
import org.apache.hadoop.hive.ql.exec.persistence.{MapJoinSingleKey, MapJoinObjectKey,
    MapJoinDoubleKeys, MapJoinObjectValue}
import org.apache.spark.serializer.{KryoRegistrator => SparkKryoRegistrator}
import shark.execution.serialization.SerializableWritable


class KryoRegistrator extends SparkKryoRegistrator {
  def registerClasses(kryo: Kryo) {

    kryo.register(classOf[execution.ReduceKey])

    // The map join data structures are Java serializable.
    kryo.register(classOf[MapJoinSingleKey], new KryoJavaSerializer)
    kryo.register(classOf[MapJoinObjectKey], new KryoJavaSerializer)
    kryo.register(classOf[MapJoinDoubleKeys], new KryoJavaSerializer)
    kryo.register(classOf[MapJoinObjectValue], new KryoJavaSerializer)

    kryo.register(classOf[SerializableWritable[_]], new KryoSWSerializer)

    // As far as I (rxin) know, among all Hadoop writables only TimestampWritable
    // cannot be serialized by Kryo out of the box.
    kryo.register(classOf[org.apache.hadoop.hive.serde2.io.TimestampWritable],
      new KryoWritableSerializer[org.apache.hadoop.hive.serde2.io.TimestampWritable])
  }
}

class KryoSWSerializer[T <: Writable] extends KSerializer[SerializableWritable[T]]  {
  def write(kryo : Kryo, out : KryoOutput, obj : SerializableWritable[T]) {
    kryo.writeClassAndObject(out, obj.t); out.flush;
  }
  def read(kryo : Kryo, in : KryoInput, cls : Class[SerializableWritable[T]]) : SerializableWritable[T] = {
    new SerializableWritable(
      kryo.readClassAndObject(in).asInstanceOf[T]
    )
  }
}

/** A Kryo serializer for Hadoop writables. */
class KryoWritableSerializer[T <: Writable] extends KSerializer[T] {
  override def write(kryo: Kryo, output: KryoOutput, writable: T) {
    val ouputStream = new DataOutputStream(output)
    writable.write(ouputStream)
  }

  override def read(kryo: Kryo, input: KryoInput, cls: java.lang.Class[T]): T = {
    val writable = cls.newInstance()
    val inputStream = new DataInputStream(input)
    writable.readFields(inputStream)
    writable
  }
}
