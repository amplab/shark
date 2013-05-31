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
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF
import org.apache.hadoop.hive.ql.exec.FunctionRegistry
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFCase
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFWhen
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

class GenericFunExprNodeCodeGen(context : CGContext, desc : ExprNodeGenericFuncDesc) 
  extends ExprNodeCodeGen[ExprNodeGenericFuncDesc](context, desc) {
  private var delegate : ExprCodeGen = _
  
  var genericUDF = getDesc().getGenericUDF()
  val children = (for (child <- getDesc().getChildExprs()) yield getContext().create(child)).toArray
  
  var childrenOIs:Array[ObjectInspector] = _
  var outputOI : ObjectInspector = _

  override def notifyEvaluatingCodeGenNeed() {
    super.notifyEvaluatingCodeGenNeed()

    for (child <- children) child.notifyEvaluatingCodeGenNeed()
  }

  def getChildren() = children

  override def create(rowInspector : ObjectInspector) : ObjectInspector = {
    // Initialize all children first
    childrenOIs = for (child <- children) yield child.initialize(rowInspector)
    if (childrenOIs.exists(_ == null))
      null
    else {
      if (isStateful() &&
        ((genericUDF.isInstanceOf[GenericUDFCase])
          || (genericUDF.isInstanceOf[GenericUDFWhen]))) {
        // the check is from the Hive ExprNodeGenericEvaluator
        throw new CGAssertRuntimeException("Stateful expressions cannot be used inside of CASE")
      }

      outputOI = genericUDF.initializeAndFoldConstants(childrenOIs)
      if (!outputOI.isInstanceOf[PrimitiveObjectInspector]) {
        // TODO result should be primitive objectinspector
        return null
      }
      if (ObjectInspectorUtils.isConstantObjectInspector(outputOI) && isDeterministic()) {
        // The output of this UDF is constant, so we create the ConstantExpr directly.
        delegate = getContext().create(outputOI.asInstanceOf[ConstantObjectInspector])
      } else {
        delegate = getContext().create(this)
      }

      if (delegate == null) {
        return null
      }

      outputOI = delegate.initialize(rowInspector)
      if (outputOI == null) {
        return null
      }

      var resultVariableType : String = null
      if (outputOI.getCategory() == Category.PRIMITIVE) {
        resultVariableType = outputOI.asInstanceOf[PrimitiveObjectInspector].getPrimitiveWritableClass().getCanonicalName()
      } else {
        resultVariableType = null //TODO ObjectInspectorUtils.getWritableObjectInspector(outputOI).
      }

      this.setCodeEvaluate(delegate.codeEvaluate())
      this.setCgValidateCheck(delegate.cgValidateCheck())

      outputOI
    }
  }

  override def constantNull() = this.delegate.constantNull()

  override def isStateful() = {
    var stateful : Boolean = FunctionRegistry.isStateful(this.genericUDF)
    for (child <- children) stateful ||= child.isStateful()

    stateful
  }

  override def isDeterministic() = {
    var deterministic : Boolean = FunctionRegistry.isDeterministic(genericUDF)
    for (child <- children) deterministic &&= child.isDeterministic()

    deterministic
  }

  override def evaluationType() = delegate.evaluationType()
  override def invalidValueExpr() = delegate.invalidValueExpr()
  override def initValueExpr() = delegate.initValueExpr()
  override def resultVariableType() = delegate.resultVariableType()
  override def resultVariableName() = delegate.resultVariableName()
  override def codeInit() = delegate.codeInit()
}