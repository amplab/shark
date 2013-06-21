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

import org.apache.hadoop.hive.ql.exec.ExprNodeGenericFuncEvaluator
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluator

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector

import shark.LogHelper


/**
 * Wrap the Executor of current expression, we try to use the CodeGen executor, unless CodeGen 
 * failed for some reason (unpredicted exception or non-supported GenericUDF and data type), if that
 * happens, we will resort to Hive Executor 
 */
class ExprNodeEvaluatorWrapper(
  private val desc: ExprNodeGenericFuncDesc,
  private val stopAsCGFailed: Boolean) extends ExprNodeEvaluator with LogHelper {

  protected var ie: Executor = _

  override def initialize(inputOI: ObjectInspector): ObjectInspector = {
    var outputOI: ObjectInspector = null

    if (inputOI.isInstanceOf[StructObjectInspector]) {
      // TODO doesn't support the map/list. now
      var entry = CGClassEntry(desc)
      ie = new CGExecutor(entry.initialize(inputOI))
      outputOI = entry.getOutputOI()
    }

    if (null == outputOI) {
      if (stopAsCGFailed) { // this would be helpful in unit test of the CodeGen 
        throw new CGAssertRuntimeException("Cannot generate the CGEvaluate instance for the expr")
      }
      
      logWarning("Will switch to the Hive ExprNodeEvaluator")
      var eval = new ExprNodeGenericFuncEvaluator(desc)
      outputOI = eval.initialize(inputOI)
      ie = HiveExecutor(eval)
    }

    outputOI
  }

  override def evaluate(row: AnyRef) = ie.evaluate(row)
}