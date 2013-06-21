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

package shark.execution.cg.node

import scala.collection.JavaConversions.collectionAsScalaIterable
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc
import org.apache.hadoop.hive.ql.plan.ExprNodeNullDesc
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF
import org.apache.hadoop.hive.ql.exec.FunctionRegistry
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFCase
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFWhen
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils
import shark.execution.cg.CGContext
import shark.execution.cg.CGAssertRuntimeException
import shark.execution.cg.ExprCodeGen
import shark.execution.cg.udf.UDFCodeGen
import shark.execution.cg.EvaluationType

class GenericFunNode(context: CGContext, desc: ExprNodeGenericFuncDesc)
  extends ExprNode[ExprNodeGenericFuncDesc](context, desc) {
  
  var children: Array[_<:ExprNode[ExprNodeDesc]] = 
    (for (child <- desc.getChildExprs()) yield getContext().create(child)).toArray
  var genericUDF = desc.getGenericUDF()
  var delegate: ExprCodeGen = _
  
  override def prepare(rowInspector: ObjectInspector): Boolean = {
    if (children.map(_.fold(rowInspector)).exists(_ == false))
      return false // cannot handle the expr in its children

    if (isStateful() &&
      ((genericUDF.isInstanceOf[GenericUDFCase])
        || (genericUDF.isInstanceOf[GenericUDFWhen]))) {
      // the check is from the Hive ExprNodeGenericEvaluator
      throw new CGAssertRuntimeException("Stateful expressions cannot be used inside of CASE")
    }

    var outputPrimitiveOI = castToPrimitiveObjectInspector(
        genericUDF.initializeAndFoldConstants(
            for (child <- children) yield child.getOutputInspector()))
    if(outputPrimitiveOI == null) {
      return false
    }
    
    setOutputInspector(outputPrimitiveOI)
    
    if (ObjectInspectorUtils.isConstantObjectInspector(outputPrimitiveOI) && 
        isDeterministic() && 
        !isStateful()) {
      var constantValue = 
        outputPrimitiveOI.asInstanceOf[ConstantObjectInspector].getWritableConstantValue()

      if (null != constantValue) {
        // The output of this UDF is constant, so we create the ConstantExpr directly.
        delegate = getContext().create(
            new ExprNodeConstantDesc(outputPrimitiveOI.getPrimitiveJavaObject(constantValue)))
      } else {
        // create Null Node and initialize with output inspector
        delegate = ConverterNode(GenericFunNode.this)
      }
    } else {
      delegate = getContext().create(GenericFunNode.this)
    }

    if (delegate != null && delegate.fold(rowInspector)) {
      // because the "evaluate" code requires the recursive reference, we need to take the 
      // lazy "generating", otherwise it may cause error for common sub expression in partial
      // evaluating
      codeEvaluateSnippet = () => delegate.cgEvaluate()
      codeValidationSnippet = ()=> delegate.cgValidateCheck()
      codeValueExpr = () => delegate.resultVariableName()
      true
    } else {
      false
    }
  }

  // Current node is stateful if current UDF or one of the child node is stateful
  override def isStateful() = {
    children.foldLeft(
        FunctionRegistry.isStateful(GenericFunNode.this.genericUDF))(_ || _.isStateful())
  }

  // Current node is deterministic, if current UDF and all children nodes are deterministic 
  override def isDeterministic() = {
    children.foldLeft(
        FunctionRegistry.isDeterministic(GenericFunNode.this.genericUDF))(_ && _.isDeterministic())
  }
  
  override def notifyEvaluatingCodeGenNeed() {
    delegate.notifyEvaluatingCodeGenNeed() 
    super.notifyEvaluatingCodeGenNeed()
  }
  
  override def notifycgValidateCheckCodeNeed() {
    delegate.notifycgValidateCheckCodeNeed()
    super.notifycgValidateCheckCodeNeed()
  }
  
  override def evaluationType() = delegate.evaluationType()
  override def invalidValueExpr() = delegate.initValueExpr()
  override def initValueExpr() = delegate.initValueExpr()
  override def resultVariableName() = delegate.resultVariableName()
  override def codeInit() = delegate.codeInit()
  override def constantNull() = delegate.constantNull()
}

object GenericFunNode {
  def apply(context: CGContext, desc: ExprNodeGenericFuncDesc) = 
    new GenericFunNode(context, desc)
}