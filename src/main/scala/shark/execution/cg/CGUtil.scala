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

import shark.execution.serialization.KryoSerializer


object CGUtil {
  
  val serializer = new KryoSerializer()

  var u: Int = 0
  // generates the class name for Union
  def randUnionClassName() = this.synchronized({ u += 1; "UUnion" + u })

  var s: Int = 0
  // generates the class name for Struct 
  def randStructClassName() = this.synchronized({ s += 1; "SStruct" + s })

  var o: Int = 0
  // generates the class name for Operator
  def randOperatorClassName() = this.synchronized({ o += 1; "OOperator" + o })
  
  var c: Int = 0
  // generate the intermediate result variable name for expression evaluator
  def randExressionEvaluatorName() = this.synchronized({ c += 1; "m" + c})
  
  var i: Int = 0
  // generate the null value indicator variable name for expression evaluator
  def randNullValueIndicatorName() = this.synchronized({ i += 1; "i" + i})
  
  var p: Int = 0
  // generate the null value indicator variable name for expression evaluator
  def randProperty() = this.synchronized({ p += 1; "p" + p})

  var e: Int = 0
  // generate the expression id
  def randExprId() = this.synchronized({e += 1; "e" + e})
  
  def serialize(o: Any): Array[Byte] = this.synchronized {
    serializer.serialize(o)
  }

  def deserialize[T](bytes: Array[Byte]): T = this.synchronized {
    serializer.deserialize(bytes)
  }
  
  def makeCGFieldName(name: String) = "__field___" + name.toLowerCase()
}
