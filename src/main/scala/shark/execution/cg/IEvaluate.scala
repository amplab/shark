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

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.hive.ql.exec.ExprNodeGenericFuncEvaluator


// interface for the generated java class
abstract class IEvaluate {
  def init(oi: ObjectInspector)
  def evaluate(row: Object): Object
}

/**
 * default implementation, always return null
 */
class NullEvaluator extends IEvaluate {
  def init(oi: ObjectInspector) {}
  def evaluate(row: Object): Object = null
}

object NullEvaluatorClass {
  def apply() = classOf[NullEvaluator].asInstanceOf[Class[IEvaluate]]
}

// interface for delegating the real Executor
abstract class Executor {
  def evaluate(rowInspector: Object): Object
}

/**
 * Code Gen Executor
 */
case class CGExecutor(private val ie: IEvaluate) extends Executor {
  override def evaluate(row: AnyRef) = ie.evaluate(row)
}

/**
 * Hive Executor
 */
case class HiveExecutor(private val ie: ExprNodeGenericFuncEvaluator) extends Executor {
  override def evaluate(row: AnyRef) = ie.evaluate(row)
}