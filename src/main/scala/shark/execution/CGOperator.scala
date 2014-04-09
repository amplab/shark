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
import shark.execution.cg.row.TENInstance
import shark.execution.cg.row.HiveTENFactory
import shark.execution.cg.CGUtil
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc

abstract class CGOperator(
    val path: String, 
    val op: Operator[_ <: HiveDesc], 
    val packageName: String = "shark.execution.cg.operator",
    val className: String = CGUtil.randOperatorClassName()) {
  def fullClassName() = packageName + "." + className
  def initial: Map[String, Any]
}

// TODO
// CGSelectOperator & CGSelectOperator2 accomplish the same goal, 
// the former output the object in generate class (which represented in CGStruct),
// the later output the object in Writable array. Ideally, the former should be faster than the 
// later in serialization / deserialization (less objects created), however, we found the former
// is slower in checking the field whether it's null. Hence we keep the both implementation for
// further improving.
/**
 * Generate the java source code for SelectOperator, the output object is the CGRow(generate class)
 */
class CGSelectOperator(override val op: SelectOperator) 
  extends CGOperator(CGOperator.CG_OPERATOR_SELECT, op) {
  override def initial: Map[String, Any] = {
    import scala.collection.JavaConversions._
    
    val ctx = new CGExprContext()
    val ten = HiveTENFactory.create(op.conf.getOutputColumnNames(), 
      op.conf.getColList(), op.cginputrows(0), op.cgrow)
    val een = TENInstance.transform(ten, ctx)
    
    Map("ctx" -> ctx, "cs" -> een)
  }
}

/**
 * Generate the java source code for SelectOperator, the output object is Writable array.
 */
class CGSelectOperator2(override val op: SelectOperator) 
  extends CGOperator(CGOperator.CG_OPERATOR_SELECT2, op) {
  override def initial: Map[String, Any] = {
    import scala.collection.JavaConversions._
    
    val ctx = new CGExprContext()
    val ten = HiveTENFactory.create(op.conf.getColList(), op.cginputrows(0), op.cgrow)
    val een = TENInstance.transform(ten, ctx)
    
    Map("ctx" -> ctx, "cs" -> een)
  }
}

/**
 * Generate the java source code for FilterOperator
 */
class CGFilterOperator(override val op: FilterOperator) 
  extends CGOperator(CGOperator.CG_OPERATOR_FILTER, op) {
  override def initial: Map[String, Any] = {
    import scala.collection.JavaConversions._
    
    val ctx = new CGExprContext()
    val ten = HiveTENFactory.create(op.getConf.getPredicate(), 
      TypeUtil.BooleanType, op.cginputrows(0))
    val een = TENInstance.transform(ten, ctx)
    
    Map("ctx" -> ctx, "cs" -> een)
  }
}

case class CGExecOperator(names: Seq[String], descs: Seq[ExprNodeDesc], input: CGStruct, output: CGStruct) 
  extends CGOperator(CGOperator.CG_EXPRESSION, null) {
  override def initial: Map[String, Any] = {
    import scala.collection.JavaConversions._
    
    val ctx = new CGExprContext()
    val ten = HiveTENFactory.create(descs, input, output)
    val een = TENInstance.transform(ten, ctx)
    
    Map("ctx" -> ctx, "cs" -> een, "cgrow" -> input)
  }
}

/*
 * Put all of the operator template here.
 */
object CGOperator {
  val CG_OPERATOR_SELECT = "shark/execution/cg/operator/cg_op_select.ssp"
  val CG_OPERATOR_SELECT2 = "shark/execution/cg/operator/cg_op_select2.ssp"
  val CG_EXPRESSION = "shark/execution/cg/operator/cg_expression.ssp"
  val CG_OPERATOR_FILTER = "shark/execution/cg/operator/cg_op_filter.ssp"
  
  def generate(cgo: CGOperator): String = {
    CGTE.layout(cgo.path, Map("op" -> cgo.op, "cgo"-> cgo) ++ cgo.initial)
  }
}
