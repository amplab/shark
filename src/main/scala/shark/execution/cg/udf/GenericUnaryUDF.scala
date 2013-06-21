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

import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc

import shark.execution.cg.node.GenericFunNode
import shark.execution.cg.EvaluationType
import shark.execution.cg.CGAssertRuntimeException
import shark.execution.cg.node.ConverterType
import shark.execution.cg.node.ConverterNode
import shark.execution.cg.ExprCodeGen
import shark.execution.cg.node.ExprNode


/**
 * Popular Unary UDFs(generic) re-implementations (not complete)
 */
sealed abstract class UnaryUDF(node: GenericFunNode)
  extends UDFCodeGen(node) 
  with UDFCallHelper {

  protected var v1: ExprNode[ExprNodeDesc] = _
  override def evaluationType() = EvaluationType.SET
  override def invalidValueExpr() = nullValueIndicatorVariableName() + "=false"
  override def initValueExpr() = nullValueIndicatorVariableName() + "=true"

  /**
   * Requires very parameter to be checked if its value is null
   */
  override protected def requireNullValueCheck(parameterIndex: Int) = true
  override protected def cgUDFCall() = cgUDFCall(v1.typeInfo)
  override protected def cgUDFCallBoolean() = {
    var code = cgUDFCallPrimitive()
    if (code != null)
      () => resultVariableName() + ".set(" + code + ");"
    else
      null
  }
  
  override protected def cgUDFCallText() = cgUDFCallPrimitive()
  override protected def cgUDFCallComparable() = cgUDFCallPrimitive()
  
  override def prepare(rowInspector: ObjectInspector, children: Array[_ <: ExprNode[ExprNodeDesc]]) = {
    if (children.size != 1) {
      throw new CGAssertRuntimeException("expected 1 arguments in the GenericUDFOPNotNullCodeGen")
    }

    v1 = ConverterNode(children(0), children(0).getOutputInspector(), ConverterType.LIMITED_TYPE, limitedTypeSet())

    Array(v1)
  }
  
  def limitedTypeSet(): Set[PrimitiveCategory]
}

/**
 * ! (NOT)
 * GenericUDFOPNotCodeGen.
 */
case class UDFOPNot(override val node: GenericFunNode)
  extends UnaryUDF(node) {
  override protected def cgUDFCallPrimitive() = "!" + v1

  def limitedTypeSet() = Set(PrimitiveCategory.BOOLEAN)
}

/**
 * IS NULL
 *
 */
case class UDFOPNull(override val node: GenericFunNode)
  extends UnaryUDF(node) {

  override protected def cgUDFCallPrimitive() = "null==" + v1
  override protected def cgUDFCallBoolean() = {
    var code = cgUDFCallPrimitive()
    if (code != null)
      () => resultVariableName() + ".set(" + code + ");"
    else
      null
  }
  
  override def invalidValueExpr(): String = null
  override def initValueExpr(): String = null
  
  override def prepare(rowInspector: ObjectInspector, children: Array[_ <: ExprNode[ExprNodeDesc]]) = {
    if (children.size != 1) {
      throw new CGAssertRuntimeException("expected 1 arguments in the GenericUDFOPNotNullCodeGen")
    }

    v1 = ConverterNode(children(0), children(0).getOutputInspector())
    // not require any validation, because it's always available
    codeValidationSnippet = ()=>null
    Array(v1)
  }
    
  override protected def requireNullValueCheck(parameterIndex: Int) = false
  def limitedTypeSet() = throw new UnsupportedOperationException("")
}

/**
 * Bit Not
 */
case class UDFOPBitNotBinaryUDF(override val node: GenericFunNode)
  extends UnaryUDF(node) {
  override protected def cgUDFCallPrimitive() = "~" + v1
  
  override protected def cgUDFCallByte() = {
    var code = cgUDFCallPrimitive()
    if (code != null)
      () => resultVariableName() + ".set((byte)" + code + ");"
    else
      null
  }
    
  override protected def cgUDFCallShort() = {
    var code = cgUDFCallPrimitive()
    if (code != null)
      () => resultVariableName() + ".set((short)" + code + ");"
    else
      null
  }
  
  def limitedTypeSet() = Set(PrimitiveCategory.BYTE, PrimitiveCategory.SHORT, PrimitiveCategory.INT, PrimitiveCategory.LONG)
}

/**
 * IS NOT NULL (isnotnull)
 */
case class UDFOPNotNull(override val node: GenericFunNode)
  extends UnaryUDF(node) {
  override protected def cgUDFCallPrimitive() = "null!=" + v1
  
  override def invalidValueExpr(): String = null
  override def initValueExpr(): String = null
  
  override def prepare(rowInspector: ObjectInspector, children: Array[_ <: ExprNode[ExprNodeDesc]]) = {
    if (children.size != 1) {
      throw new CGAssertRuntimeException("expected 1 arguments in the GenericUDFOPNotNullCodeGen")
    }

    v1 = ConverterNode(children(0), children(0).getOutputInspector())
    // not require any validation, because it's always available
    codeValidationSnippet = ()=>null
    Array(v1)
  }
    
  override protected def requireNullValueCheck(parameterIndex: Int) = false
  def limitedTypeSet() = throw new UnsupportedOperationException("")
}

/**
 * +
 */
case class UDFOPPositive(override val node: GenericFunNode)
  extends UnaryUDF(node) {
  override protected def cgUDFCallPrimitive() = v1.toString()
  
  def limitedTypeSet() = Set(PrimitiveCategory.BYTE, PrimitiveCategory.SHORT, PrimitiveCategory.FLOAT, PrimitiveCategory.INT, PrimitiveCategory.LONG, PrimitiveCategory.DOUBLE)
}

/**
 * -
 */
case class UDFOPNegative(override val node: GenericFunNode)
  extends UnaryUDF(node) {
  override protected def cgUDFCallPrimitive() = "-" + v1
  
  def limitedTypeSet() = Set(PrimitiveCategory.BYTE, PrimitiveCategory.SHORT, PrimitiveCategory.FLOAT, PrimitiveCategory.INT, PrimitiveCategory.LONG, PrimitiveCategory.DOUBLE)
}
