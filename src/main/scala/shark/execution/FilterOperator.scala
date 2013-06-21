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

import scala.collection.Iterator
import scala.reflect.BeanProperty
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluator
import org.apache.hadoop.hive.ql.exec.{FilterOperator => HiveFilterOperator}
import org.apache.hadoop.hive.ql.metadata.HiveException
import org.apache.hadoop.hive.ql.plan.FilterDesc
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector
import shark.execution.cg.CGEvaluatorFactory
import shark.SharkConfVars


class FilterOperator extends UnaryOperator[HiveFilterOperator] {

  @transient var conditionEvaluator: ExprNodeEvaluator = _
  @transient var conditionInspector: PrimitiveObjectInspector = _

  @BeanProperty var conf: FilterDesc = _
  @BeanProperty var useCG = true
  
  override def initializeOnMaster() {
    conf = hiveOp.getConf()
    useCG = SharkConfVars.getBoolVar(super.hconf, SharkConfVars.EXPR_CG)
  }

  override def initializeOnSlave() {
    try {
      conditionEvaluator = CGEvaluatorFactory.get(conf.getPredicate(), useCG)

      conditionInspector = conditionEvaluator.initialize(objectInspector)
        .asInstanceOf[PrimitiveObjectInspector]
    } catch {
      case e: Throwable => throw new HiveException(e)
    }
  }

  override def processPartition(split: Int, iter: Iterator[_]) = {
    iter.filter { row =>
      java.lang.Boolean.TRUE.equals(
        conditionInspector.getPrimitiveJavaObject(conditionEvaluator.evaluate(row)))
    }
  }

}