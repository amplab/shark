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

import java.util.UUID
import org.apache.hadoop.hive.ql.exec.ExprNodeGenericFuncEvaluator
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluator

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector

import shark.LogHelper

class ExprNodeEvaluatorWrapper(desc : ExprNodeGenericFuncDesc, private val stopAsCGFailed:Boolean = true) extends ExprNodeEvaluator with LogHelper {
  private val PACKANGE_NAME : String = "org.apache.hadoop.hive.ql.exec.bytecode.example"
  protected var ie : Executor = _
  protected var cgm : CodeGenManager = new CodeGenManager(PACKANGE_NAME, getRandomClassName(), desc)

  private def getRandomClassName() = "GEN" + UUID.randomUUID().toString().replaceAll("\\-", "_")

  override def initialize(inputOI : ObjectInspector) : ObjectInspector = {
    var outputOI : ObjectInspector = null

    if (inputOI.isInstanceOf[StructObjectInspector]) {
      // not support the map/list. currently
      try {
        outputOI = this.cgm.getOutputOI(inputOI)
        if (outputOI != null) {
          var eval = cgm.evaluator()
          eval.init(inputOI)
          ie = CGExecutor(eval)
        }
      } catch {
        // if anything wrong with the code gen, then switch off the code gen
        case ioe : Throwable => {
          ioe.printStackTrace(errStream())
          outputOI = null
        }
      }
    }

    if (null == outputOI) {
      if (stopAsCGFailed) throw new CGAssertRuntimeException ("Can not generating the code for the expr")
      logError("Will switch to the Hive ExprNodeEvaluator")
      var eval = new ExprNodeGenericFuncEvaluator(desc)
      outputOI = eval.initialize(inputOI)
      ie = InterpretExecutor(eval)
    } 

    outputOI
  }

  override def evaluate(row : AnyRef) = ie.evaluate(row)
}