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

import scala.language.implicitConversions

import org.apache.hadoop.hive.ql.plan.OperatorDesc

import shark.execution.serialization.KryoSerializationWrapper
import shark.execution.serialization.OperatorSerializationWrapper

package object execution {

  type HiveDesc = OperatorDesc // XXXDesc in Hive is the subclass of Serializable

  implicit def opSerWrapper2op[T <: Operator[_ <: HiveDesc]](
      wrapper: OperatorSerializationWrapper[T]): T = wrapper.value

  implicit def kryoWrapper2object[T](wrapper: KryoSerializationWrapper[T]): T = wrapper.value
}
