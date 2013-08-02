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

import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluator
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc
import org.apache.hadoop.hive.ql.plan.ExprNodeFieldDesc
import org.apache.hadoop.hive.ql.plan.ExprNodeNullDesc
import org.apache.hadoop.hive.ql.exec.ExprNodeConstantEvaluator
import org.apache.hadoop.hive.ql.exec.ExprNodeColumnEvaluator
import org.apache.hadoop.hive.ql.exec.ExprNodeFieldEvaluator
import org.apache.hadoop.hive.ql.exec.ExprNodeNullEvaluator
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluatorFactory

import shark.LogHelper

/**
 * Entry point of the Code Gen Evaluator
 */
object CGEvaluatorFactory extends LogHelper {
  /**
   * @param desc ExprNode Description
   * @param useCG true for retrieving the CodeGen evaluator, otherwise Hive Expr Evaluator 
   */
  def get(desc: ExprNodeDesc, useCG: Boolean): ExprNodeEvaluator = {
    if (useCG) {
      logInfo("Using CodeGen for Expression Evaluating")
      getEvaluator(desc, false)
    } else {
      logInfo("Using Hive ExprEvaluator for Expression Evaluating")
      ExprNodeEvaluatorFactory.get(desc)
    }
  }

  /**
   * @param desc ExprNode Description
   * @param stopAsCGFailed true for throw exception if fail in creating CodeGen evaluator, 
   *        otherwise, will resort to Hive expr evaluator. Basically, the flag is set to true
   *        only for unittesting the CodeGen evaluator purpose.
   */
  def getEvaluator(desc: ExprNodeDesc, stopAsCGFailed: Boolean = true): ExprNodeEvaluator = {
    desc match {
      case x: ExprNodeConstantDesc    => new ExprNodeConstantEvaluator(x)
      case x: ExprNodeColumnDesc      => new ExprNodeColumnEvaluator(x)
      case x: ExprNodeGenericFuncDesc => new ExprNodeEvaluatorWrapper(x, stopAsCGFailed)
      case x: ExprNodeFieldDesc       => new ExprNodeFieldEvaluator(x)
      case x: ExprNodeNullDesc        => new ExprNodeNullEvaluator(x)
      case _ => throw new CGAssertRuntimeException("Cannot map NodeDesc for " + desc)
    }
  }
}