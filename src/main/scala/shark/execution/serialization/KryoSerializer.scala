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
import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.Input
import com.esotericsoftware.kryo.io.Output


/**
 * Java object serialization using Kryo. This is much more efficient, but Kryo
 * sometimes is buggy to use. We use this mainly to serialize the object
 * inspectors.
 */
object KryoSerializer {

  @transient val ser = new spark.KryoSerializer

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

class KryoSerializer(klasses: Array[Class[_]], cl: ClassLoader) {
  val kryo = new Kryo()
  val input = new Input()
  val output = new Output()
  {
    klasses.foreach( cls => kryo.register(cls))
  }
  
  def this(klasses: Array[Class[_]]) = {
    this(klasses, Thread.currentThread().getContextClassLoader())
  }
  
  def serialize[T](o: T): Array[Byte] = {
    output.clear()
    kryo.writeObject(output, o)
    output.getBuffer()
  }

  def deserialize[T](bytes: Array[Byte], cls: Class[_]): T  = {
    input.setBuffer(bytes)
    kryo.readObject(input, cls).asInstanceOf[T]
  }
}
