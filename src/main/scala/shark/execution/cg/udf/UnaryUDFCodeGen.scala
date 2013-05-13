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

package shark.execution.cg.udf

import org.apache.hadoop.hive.ql.plan.ExprNodeDesc
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector

import shark.execution.cg.node.GenericFunExprNodeCodeGen
import shark.execution.cg.EvaluationType
import shark.execution.cg.node.ExprNodeCodeGen

sealed abstract class UnaryUDFCodeGen(node : GenericFunExprNodeCodeGen) 
  extends UDFCodeGen(node) {

  override def evaluationType() = EvaluationType.SET
  override def invalidValueExpr() = nullValueIndicatorVariableName() + "=false"
  override def initValueExpr() = nullValueIndicatorVariableName() + "=true"

  /**
   * Requires very parameter to be checked if its value is null
   */
  override protected def requireNullValueCheck(parameterIndex : Int) = true
  override protected def cgUDFCall(nodes : Array[ExprNodeCodeGen[ExprNodeDesc]]) = 
    resultVariableName() + ".set(" + cgUDFCall(nodes(0)) + ");"

  protected def cgUDFCall(v1 : ExprNodeCodeGen[ExprNodeDesc]) : String
}
/**
 * ! (NOT)
 * GenericUDFOPNotCodeGen.
 */
class GenericUDFOPNotCodeGen(node : GenericFunExprNodeCodeGen) 
  extends UnaryUDFCodeGen(node) {
  override protected def cgUDFCall(v1 : ExprNodeCodeGen[ExprNodeDesc]) = "!" + getExprValue(v1)
}

/**
 * IS NULL
 *
 */
class GenericUDFOPNullCodeGen(node : GenericFunExprNodeCodeGen) 
  extends UnaryUDFCodeGen(node) {
  this.setCgValidateCheck(null)

  override protected def cgUDFCall(v1 : ExprNodeCodeGen[ExprNodeDesc]) = "null==" + v1.resultVariableName()
  override def invalidValueExpr() : String = null
  override def initValueExpr() : String = null
  override protected def requireNullValueCheck(parameterIndex : Int) = false
}

/**
 * Bit Not
 */
class UDFOPBitNotBinaryUDFCodeGen(node : GenericFunExprNodeCodeGen) 
  extends UnaryUDFCodeGen(node) {
  override protected def cgUDFCall(v1 : ExprNodeCodeGen[ExprNodeDesc]) = "~" + getExprValue(v1)
}

/**
 * IS NOT NULL
 *
 */
class GenericUDFOPNotNullCodeGen(node : GenericFunExprNodeCodeGen) 
  extends UnaryUDFCodeGen(node) {
  this.setCgValidateCheck(null)

  override protected def cgUDFCall(v1 : ExprNodeCodeGen[ExprNodeDesc]) = "null!=" + v1.resultVariableName()
  override def invalidValueExpr() : String = null
  override def initValueExpr() : String = null
  override protected def requireNullValueCheck(parameterIndex : Int) = false
}

/**
 * +
 *
 */
class UDFOPPositiveCodeGen(node : GenericFunExprNodeCodeGen) 
  extends UnaryUDFCodeGen(node) {
  override protected def cgUDFCall(v1 : ExprNodeCodeGen[ExprNodeDesc]) = getExprValue(v1)
}

/**
 * -
 *
 */
class UDFOPNegativeCodeGen(node : GenericFunExprNodeCodeGen) 
  extends UnaryUDFCodeGen(node) {
  override protected def cgUDFCall(v1 : ExprNodeCodeGen[ExprNodeDesc]) = "-" + getExprValue(v1)
}
