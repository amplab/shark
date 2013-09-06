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

package shark.execution.cg.operator

import shark.execution.cg.row.CGTE
import shark.execution.cg.row.CGRowUtil
import shark.execution.cg.row.CGStruct
import shark.execution.cg.CGObjectOperator
import shark.execution.TableScanOperator
import shark.execution.JoinOperator
import shark.execution.Operator
import shark.execution.HiveDesc
import shark.execution.ReduceSinkOperator
import shark.execution.GroupByPreShuffleOperator
import shark.execution.GroupByPostShuffleOperator

abstract class CGOperator(
    val path: String, 
    val op: Operator[_ <: HiveDesc], 
    val packageName: String = "shark.execution.cg.operator",
    val className: String = CGRowUtil.operatorClassName()) {
  def fullClassName() = packageName + "." + className
}

class CGJoinOperator(val row: CGStruct, override val op: JoinOperator) 
  extends CGOperator(CGOperator.CG_OPERATOR_JOIN, op)

/*
 * Currently only support the NO DISTINCT 
 */
class CGReduceSinkOperator(
    val row: CGStruct, 
    override val op: ReduceSinkOperator, 
    path: String) 
  extends CGOperator(path, op)

/*
 * org.apache.hadoop.hive.ql.exec.KeyWrapperFactory in the PreShuffleOperator is package visibility
 * In order to call the method of KeyWrapperFactory/ KeyWrapper, we have to put the generated code
 * in the same package
 */
class CGGroupByPreShuffleOperator(
    val row: CGStruct, 
    override val op: GroupByPreShuffleOperator) 
  extends CGOperator(CGOperator.CG_GROUPBYPRESHUFFLEOPERATOR, op)

/*
 * org.apache.hadoop.hive.ql.exec.KeyWrapperFactory in the PostShuffleOperator is package visibility
 * In order to call the method of KeyWrapperFactory/ KeyWrapper, we have to put the generated code
 * in the same package
 */
class CGGroupByPostShuffleOperator(
    val row: CGStruct, 
    override val op: GroupByPostShuffleOperator, 
    path: String) 
  extends CGOperator(path, op)

object CGOperator {
  val CG_OPERATOR_JOIN = "shark/execution/cg/operator/cg_joinoperator.ssp"
  val CG_OPERATOR_REDUCE_SINK_NODISTINCT = "shark/execution/cg/operator/cg_reducesinkoperator_nodistinct.ssp"
  val CG_OPERATOR_REDUCE_SINK_DISTINCT = "haven't implemented yet, will cause exception"
  val CG_GROUPBYPRESHUFFLEOPERATOR = "shark/execution/cg/operator/cg_groupbypreshuffleoperator.ssp"
  val CG_GROUPBYPOSTSHUFFLEOPERATOR_NODISTINCT = "shark/execution/cg/operator/cg_groupbypostshuffleoperator_nodistinct.ssp"
  val CG_GROUPBYPOSTSHUFFLEOPERATOR_DISTINCT = "haven't implemented yet, will cause exception"
  
  def generate(cgo: CGOperator): String = {
    CGTE.layout(cgo.path, Map("op" -> cgo.op, "cgo"-> cgo))
  }
}