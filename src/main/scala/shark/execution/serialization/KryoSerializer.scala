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

import java.nio.ByteBuffer

import org.apache.spark.{SparkEnv, SparkConf}
import org.apache.spark.serializer.{KryoSerializer => SparkKryoSerializer}
import org.apache.commons.io.output.ByteArrayOutputStream

import com.esotericsoftware.kryo.io.Output
import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.Input
import com.esotericsoftware.kryo.Serializer

import shark.SharkContext

/**
 * Java object serialization using Kryo. This is much more efficient, but Kryo
 * sometimes is buggy to use. We use this mainly to serialize the object
 * inspectors.
 */
object KryoSerializer {

  @transient var ser = new SparkKryoSerializer(new SparkConf())

  def initWithSharkContext(sc: SharkContext) {
  	ser = new SparkKryoSerializer(sc.sparkEnv.conf)
  }

  def initWithSparkConf(sparkConf: SparkConf) {
  	ser = new SparkKryoSerializer(sparkConf)
  }

  def serialize[T](o: T): Array[Byte] = {
    ser.newInstance().serialize(o).array()
  }

  def deserialize[T](bytes: Array[Byte]): T  = {
    ser.newInstance().deserialize[T](ByteBuffer.wrap(bytes))
  }
  
  def deserialize[T](bytes: Array[Byte], cl: ClassLoader): T  = {
    ser.newInstance().deserialize[T](ByteBuffer.wrap(bytes), cl)
  }
}

/**
 * This class is used by the genereated classes to ser/de objects (which implement KryoSerializable)
 */
class KryoSerializer(klasses: Array[Class[_]] = Array[Class[_]](), 
    var cl: ClassLoader = Thread.currentThread().getContextClassLoader()) {
  
  val kryo = new SparkKryoSerializer().newKryo()

  val output = new Output(new ByteArrayOutputStream())
  val input = new Input()
  
  {
    // register the generated class
    klasses.foreach(cls => kryo.register(cls))
  }

  def deserialize[T](bytes: Array[Byte]): T = deserialize(bytes, cl)

  def deserialize[T](bytes: Array[Byte], classloader: ClassLoader) = {
    val oldClassLoader = kryo.getClassLoader
    try {
      kryo.setClassLoader(classloader)
      input.setBuffer(bytes)
      val obj = kryo.readClassAndObject(input).asInstanceOf[T]
      
      obj
    } finally {
      kryo.setClassLoader(oldClassLoader)
    }
  }
  
  def serialize[T](o: T): Array[Byte] = { 
    output.clear()
    kryo.writeClassAndObject(output, o)
    
    output.toBytes
  }
}
