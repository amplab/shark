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
import java.util.HashMap

abstract class ExprNode[+T <: ExprNodeDesc](context: CGContext, val desc: T)
  extends ExprCodeGen(context) {

  // flag for common sub expression, if it's evaluated, then no need to re-generate the code
  private var evaluateCodeGenerated = false
  private var initialized = false
  private var canHandleOI = false
  
  override def evaluationType() = EvaluationType.GET
  override def codeInit(): LinkedHashSet[String] = new LinkedHashSet[String]()
  
  override def notifyEvaluatingCodeGenNeed() {
    evaluateCodeGenerated = false
    notifycgValidateCheckCodeNeed()
  }
  
  override final def cgEvaluate(): String = {
    if (constantNull()) {
      // no need to add code for expr evaluating
      return null
    }

    if (evaluationType() == EvaluationType.CONSTANT &&
      isDeterministic() &&
      !isStateful()) {
      // if the evaluationType is CONSTANT, then the evaluating code
      // basically is the constant variable.
      return codeEvaluateSnippet()
    }

    if (!evaluateCodeGenerated) {
      // if it was not generated, then generate the code.
      // be in mind, the value could be changed if need to re-gen the code.
      evaluateCodeGenerated = true
      return codeEvaluateSnippet()
    }

    null
  }

  override final def fold(rowInspector: ObjectInspector): Boolean = {
    if (!initialized){
      initialized = true
      canHandleOI = prepare(rowInspector)
    }
    
    canHandleOI
  }
  
  /**
   * Sub class would implement the method for initializing. This method is called at most once for
   * each Node instance.
   * @return false indicate the CG is not able to handle the ObjectInspector / UDF, otherwise true
   */
  def prepare(rowInspector: ObjectInspector): Boolean
}
