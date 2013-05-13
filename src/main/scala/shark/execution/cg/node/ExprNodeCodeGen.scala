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

import scala.collection.mutable.LinkedHashSet
import scala.reflect.BeanProperty

import org.apache.hadoop.hive.ql.plan.ExprNodeDesc
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector

import shark.execution.cg.ExprCodeGen
import shark.execution.cg.CGContext
import shark.execution.cg.EvaluationType

abstract class ExprNodeCodeGen[+T <: ExprNodeDesc](context : CGContext, @BeanProperty val desc : T) extends ExprCodeGen(context) {
  private var outputInspector : ObjectInspector = null

  override def evaluationType() = EvaluationType.GET

  def isStateful() = false

  def isDeterministic() = true

  private[this] var evaluateCodeGenerated = false
  private[this] var codeEvaluate : String = null

  override final def codeEvaluate() : String = {
    if (constantNull()) {
      // no need to add code for expr evaluating
      return null
    }

    if (evaluationType() == EvaluationType.CONSTANT) {
      // if the evaluationType is CONSTANT, then the evaluating code
      // basically is the constant variable.
      return codeEvaluate
    }

    if (!evaluateCodeGenerated) {
      // if it was not generated, then generate the code.
      // be in mind, the value can be changed if need to re-gen the code.
      evaluateCodeGenerated = true
      return codeEvaluate
    }

    null
  }

  override def notifyEvaluatingCodeGenNeed() {
    evaluateCodeGenerated = false
    super.notifyEvaluatingCodeGenNeed()
  }

  protected def setCodeEvaluate(codeEvaluate : String) {
    evaluateCodeGenerated = false
    this.codeEvaluate = codeEvaluate
  }

  override final def initialize(rowInspector : ObjectInspector) = {
    if (outputInspector == null) {
      outputInspector = create(rowInspector);
    }

    outputInspector;
  }

  final def getOutputOI() = outputInspector

  protected def create(rowInspector : ObjectInspector) : ObjectInspector

  override def resultVariableType() : Class[_] = null

  override def resultVariableName() : String = null

  override def codeInit() : LinkedHashSet[String] = new LinkedHashSet[String]()
}