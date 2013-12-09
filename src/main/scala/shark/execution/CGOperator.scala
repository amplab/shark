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

package shark.execution

import shark.execution.cg.row.CGTE
import shark.execution.cg.row.CGStruct
import shark.execution.cg.CGUtil

class CGOperator(
    val path: String, 
    val op: Operator[_ <: HiveDesc], 
    val packageName: String = "shark.execution.cg.operator",
    val className: String = CGUtil.randOperatorClassName()) {
  def fullClassName() = packageName + "." + className
}

class CGSelectOperator(val row: CGStruct, override val op: SelectOperator) 
  extends CGOperator(CGOperator.CG_OPERATOR_SELECT, op)

object CGOperator {
  val CG_OPERATOR_SELECT = "shark/execution/cg/operator/cg_op_select.ssp"
  
  def generate(cgo: CGOperator): String = {
    CGTE.layout(cgo.path, Map("op" -> cgo.op, "cgo"-> cgo))
  }
}