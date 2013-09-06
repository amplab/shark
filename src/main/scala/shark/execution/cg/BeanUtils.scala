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

package shark.execution.cg

import com.esotericsoftware.kryo.KryoSerializable
import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.Output
import com.esotericsoftware.kryo.io.Input
import java.util.Map
import scala.collection.mutable.{HashMap,SynchronizedMap}

object CGBeanUtils {
  def instance[T](clz: String, args: Array[Object]): T = 
    Thread.currentThread().getContextClassLoader().loadClass(clz).getDeclaredConstructors()(0).newInstance(args: _*).asInstanceOf[T]
  
  def instance[T](clz: String): T = Thread.currentThread().getContextClassLoader().loadClass(clz).newInstance().asInstanceOf[T]
}
