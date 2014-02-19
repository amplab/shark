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
import shark.execution.cg.row.CGExprContext
import shark.execution.cg.row.TypeUtil

import shark.execution.cg.CGUtil
import shark.execution.cg.row.TENInstance

abstract class CGOperator(
    val path: String, 
    val op: Operator[_ <: HiveDesc], 
    val packageName: String = "shark.execution.cg.operator",
    val className: String = CGUtil.randOperatorClassName()) {
  def fullClassName() = packageName + "." + className
  def initial: Map[String, Any]
}

class CGSelectOperator(override val op: SelectOperator) 
  extends CGOperator(CGOperator.CG_OPERATOR_SELECT, op) {
  override def initial: Map[String, Any] = {
    import scala.collection.JavaConversions._
    
    val ctx = new CGExprContext()
    val ten = TENInstance.create(op.conf.getOutputColumnNames(), 
      op.conf.getColList(), op.cginputrows(0), op.cgrow)
    val een = TENInstance.transform(ten, ctx)
    
    Map("ctx" -> ctx, "cs" -> een)
  }
}

class CGFilterOperator(override val op: FilterOperator) 
  extends CGOperator(CGOperator.CG_OPERATOR_FILTER, op) {
  override def initial: Map[String, Any] = {
    import scala.collection.JavaConversions._
    
    val ctx = new CGExprContext()
    val ten = TENInstance.create(op.getConf.getPredicate(), TypeUtil.BooleanType, op.cginputrows(0))
    val een = TENInstance.transform(ten, ctx)
    
    Map("ctx" -> ctx, "cs" -> een)
  }
}

object CGOperator {
  val CG_OPERATOR_SELECT = "shark/execution/cg/operator/cg_op_select.ssp"
  val CG_OPERATOR_FILTER = "shark/execution/cg/operator/cg_op_filter.ssp"
  
  def generate(cgo: CGOperator): String = {
    CGTE.layout(cgo.path, Map("op" -> cgo.op, "cgo"-> cgo) ++ cgo.initial)
  }
}