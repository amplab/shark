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

import shark.execution.HiveDesc
import shark.execution.Operator


/**
 * A wrapper around our operators so they can be serialized by standard Java
 * serialization. This really just delegates the serialization of the operators
 * to XML, and that of object inspectors to Kryo.
 *
 * Use OperatorSerializationWrapper(operator) to create a wrapper.
 */
class OperatorSerializationWrapper[T <: Operator[_ <: HiveDesc]]
  extends Serializable with shark.LogHelper {

  /** The operator we are going to serialize. */
  @transient var _value: T = _

  /** The operator serialized by the XMLEncoder, minus the object inspectors. */
  var opSerialized: Array[Byte] = _

  /** The object inspectors, serialized by Kryo. */
  var objectInspectorsSerialized: Array[Byte] = _

  def value: T = {
    if (_value == null) {
      assert(opSerialized != null)
      assert(opSerialized.length > 0)
      assert(objectInspectorsSerialized != null)
      assert(objectInspectorsSerialized.length > 0)
      _value = XmlSerializer.deserialize[T](opSerialized)
      _value.objectInspectors = KryoSerializer.deserialize(objectInspectorsSerialized)
    }
    _value
  }

  def value_= (v: T):Unit = {
    _value = v
    opSerialized = XmlSerializer.serialize(value, v.hconf)
    objectInspectorsSerialized = KryoSerializer.serialize(value.objectInspectors)
  }

  override def toString(): String = {
    if (value != null) {
      "OperatorSerializationWrapper[ " + value.toString() + " ]"
    } else {
      super.toString()
    }
  }
}


object OperatorSerializationWrapper {
  def apply[T <: Operator[_ <: HiveDesc]](value: T): OperatorSerializationWrapper[T] = {
    val wrapper = new OperatorSerializationWrapper[T]
    wrapper.value = value
    wrapper
  }
}
