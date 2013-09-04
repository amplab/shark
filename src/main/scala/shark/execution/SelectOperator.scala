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

import scala.reflect.BeanProperty
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluator
import org.apache.hadoop.hive.ql.plan.SelectDesc
import shark.execution.cg.CGEvaluatorFactory
import shark.SharkConfVars
import shark.execution.cg.CompilationContext

/**
 * An operator that does projection, i.e. selecting certain columns and
 * filtering out others.
 */
class SelectOperator extends UnaryOperator[SelectDesc] {

  @BeanProperty var conf: SelectDesc = _
  @transient var evals: Array[ExprNodeEvaluator] = _
  @BeanProperty var cg: Boolean = _

  // TODO to make the evals as initialized only on Master
  override def initializeOnMaster(cc: CompilationContext) {
    super.initializeOnMaster(cc)
    conf = desc
    initializeEvals(false)
    cg = cc.useCG
  }
  
  def initializeEvals(initializeEval: Boolean) {
    if (!conf.isSelStarNoCompute) {
      import scala.collection.JavaConversions._
      evals = conf.getColList().map(CGEvaluatorFactory.get(_, cg)).toArray
      if (initializeEval) {
        evals.foreach(_.initialize(objectInspector))
      }
    }
  }
  override def initializeOnSlave() {
    initializeEvals(true)
  }

  override def processPartition(split: Int, iter: Iterator[_]) = {
    if (conf.isSelStarNoCompute) {
      iter
    } else {
      val reusedRow = new Array[Object](evals.length)
      iter.map { row =>
        var i = 0
        while (i < evals.length) {
          reusedRow(i) = evals(i).evaluate(row)
          i += 1
        }
        reusedRow
      }
    }
  }
  
  override def outputObjectInspector(): ObjectInspector = {
    if (conf.isSelStarNoCompute()) 
      super.outputObjectInspector()
    else
      initEvaluatorsAndReturnStruct(evals, conf.getOutputColumnNames(), objectInspector)
  }
}
