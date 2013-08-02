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
import org.apache.hadoop.hive.ql.plan.ExprNodeNullDesc
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory
import shark.execution.cg.CGContext
import shark.execution.cg.ValueType
import shark.execution.cg.CGAssertRuntimeException
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableVoidObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantDateObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantTimestampObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantStringObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantShortObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantLongObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantIntObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantFloatObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantDoubleObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantByteObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantBooleanObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.StandardConstantListObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.StandardConstantMapObjectInspector
import shark.execution.cg.EvaluationType

class NullNode(context: CGContext, desc: ExprNodeNullDesc) 
  extends ExprNode[ExprNodeNullDesc](context, desc) {
  // TODO should not be used in CG
  override def prepare(rawInspector: ObjectInspector) = false
}

object NullNode {
  def apply(context: CGContext, desc: ExprNodeNullDesc) = 
    new NullNode(context, desc)
}