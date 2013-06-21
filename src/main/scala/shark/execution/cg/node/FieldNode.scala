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

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.hive.ql.plan.ExprNodeFieldDesc
import shark.execution.cg.CGContext
import shark.execution.cg.CGAssertRuntimeException

class FieldNode(context: CGContext, desc: ExprNodeFieldDesc) 
  extends ExprNode[ExprNodeFieldDesc](context, desc) {
  
  // TODO not supported yet
  override def prepare(rowInspector: ObjectInspector) = false
}

object FieldNode {
  def apply(context: CGContext, desc: ExprNodeFieldDesc) = 
    new FieldNode(context, desc)
}