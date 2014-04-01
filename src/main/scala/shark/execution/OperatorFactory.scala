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

import scala.collection.JavaConversions._

import org.apache.hadoop.hive.ql.exec.{GroupByPostShuffleOperator, GroupByPreShuffleOperator}
import org.apache.hadoop.hive.ql.exec.{Operator => HOperator}
import org.apache.hadoop.hive.ql.exec.PartitionTableFunctionOperator
import org.apache.hadoop.hive.ql.metadata.HiveException

import shark.LogHelper
import shark.memstore2.CacheType._

/**
 * Given a Hive plan, OperatorFactory creates the corresponding Shark plan.
 */
object OperatorFactory extends LogHelper {

  /**
   * Given a Hive plan (that is any operator in the tree), create the plan that
   * uses Shark operators. This function automatically finds the Hive terminal
   * operator, and replicate the plan recursively up.
   */
  def createSharkPlan[T <: HiveDesc](hiveOp: HOperator[T]): TerminalOperator = {
    val hiveTerminalOp = _findHiveTerminalOperator(hiveOp)
    _createOperatorTree(hiveTerminalOp).asInstanceOf[TerminalOperator]
  }

  def createSharkMemoryStoreOutputPlan(
      hiveTerminalOp: HOperator[_ <: HiveDesc],
      tableName: String,
      databaseName: String,
      numColumns: Int,
      hivePartitionKeyOpt: Option[String],
      cacheMode: CacheType,
      isInsertInto: Boolean): TerminalOperator = {
    // TODO the terminal operator is the FileSinkOperator in Hive?
    val hiveOp = hiveTerminalOp.asInstanceOf[org.apache.hadoop.hive.ql.exec.FileSinkOperator]
    val sinkOp = _newOperatorInstance(
      classOf[MemoryStoreSinkOperator], hiveOp).asInstanceOf[MemoryStoreSinkOperator]
    sinkOp.localHiveOp = hiveOp
    sinkOp.tableName = tableName
    sinkOp.databaseName = databaseName
    sinkOp.numColumns = numColumns
    sinkOp.cacheMode = cacheMode
    sinkOp.hivePartitionKeyOpt = hivePartitionKeyOpt
    sinkOp.isInsertInto = isInsertInto
    _createAndSetParents(sinkOp, hiveTerminalOp.getParentOperators).asInstanceOf[TerminalOperator]
  }

  def createSharkFileOutputPlan(hiveTerminalOp: HOperator[_ <: HiveDesc]): TerminalOperator = {
    // TODO the terminal operator is the FileSinkOperator in Hive?
    val hiveOp = hiveTerminalOp.asInstanceOf[org.apache.hadoop.hive.ql.exec.FileSinkOperator]
    val sinkOp = _newOperatorInstance(classOf[FileSinkOperator], 
      hiveOp).asInstanceOf[TerminalOperator]
    sinkOp.localHiveOp = hiveOp
    _createAndSetParents(sinkOp, hiveTerminalOp.getParentOperators).asInstanceOf[TerminalOperator]
  }

  def createSharkRddOutputPlan(hiveTerminalOp: HOperator[_ <: HiveDesc]): TerminalOperator = {
    // TODO the terminal operator is the FileSinkOperator in Hive?
    val hiveOp = hiveTerminalOp.asInstanceOf[org.apache.hadoop.hive.ql.exec.FileSinkOperator]
    val sinkOp = _newOperatorInstance(classOf[TableRddSinkOperator], 
      hiveOp).asInstanceOf[TableRddSinkOperator]
    sinkOp.localHiveOp = hiveOp
    _createAndSetParents(sinkOp, hiveTerminalOp.getParentOperators).asInstanceOf[TerminalOperator]
  }

  /** Create a Shark operator given the Hive operator. */
  private def createSingleOperator[T <: HiveDesc](hiveOp: HOperator[T]): Operator[T] = {
    // This is kind of annoying, but it works with strong typing ...
    val sharkOp = hiveOp match {
      case hop: org.apache.hadoop.hive.ql.exec.TableScanOperator =>
        val op = _newOperatorInstance(classOf[TableScanOperator], hop)
        op.asInstanceOf[TableScanOperator].hiveOp = hop
        op
      case hop: org.apache.hadoop.hive.ql.exec.SelectOperator =>
        _newOperatorInstance(classOf[SelectOperator], hop)
      case hop: org.apache.hadoop.hive.ql.exec.FileSinkOperator =>
        val op = _newOperatorInstance(classOf[TerminalOperator], hop)
        op.asInstanceOf[TerminalOperator].localHiveOp = hop
        op
      case hop: org.apache.hadoop.hive.ql.exec.LimitOperator =>
        _newOperatorInstance(classOf[LimitOperator], hop)
      case hop: org.apache.hadoop.hive.ql.exec.FilterOperator =>
        _newOperatorInstance(classOf[FilterOperator], hop)
      case hop: org.apache.hadoop.hive.ql.exec.ReduceSinkOperator =>
        _newOperatorInstance(classOf[ReduceSinkOperator], hop)
      case hop: org.apache.hadoop.hive.ql.exec.ExtractOperator =>
        _newOperatorInstance(classOf[ExtractOperator], hop)
      case hop: org.apache.hadoop.hive.ql.exec.UnionOperator =>
        _newOperatorInstance(classOf[UnionOperator], hop)
      case hop: org.apache.hadoop.hive.ql.exec.JoinOperator =>
        _newOperatorInstance(classOf[JoinOperator], hop)
      case hop: org.apache.hadoop.hive.ql.exec.MapJoinOperator =>
        _newOperatorInstance(classOf[MapJoinOperator], hop)
      case hop: org.apache.hadoop.hive.ql.exec.ScriptOperator =>
        val op = _newOperatorInstance(classOf[ScriptOperator], hop)
        op.asInstanceOf[ScriptOperator].operatorId = hop.getOperatorId()
        op
      case hop: org.apache.hadoop.hive.ql.exec.LateralViewForwardOperator =>
        _newOperatorInstance(classOf[LateralViewForwardOperator], hop)
      case hop: org.apache.hadoop.hive.ql.exec.LateralViewJoinOperator =>
        _newOperatorInstance(classOf[LateralViewJoinOperator], hop)
      case hop: org.apache.hadoop.hive.ql.exec.UDTFOperator =>
        _newOperatorInstance(classOf[UDTFOperator], hop)
      case hop: org.apache.hadoop.hive.ql.exec.ForwardOperator =>
        _newOperatorInstance(classOf[ForwardOperator], hop)
      case hop: org.apache.hadoop.hive.ql.exec.GroupByOperator => {
        // For GroupBy, we separate post shuffle from pre shuffle.
        if (GroupByOperator.isPostShuffle(hop)) {
          _newOperatorInstance(classOf[GroupByPostShuffleOperator], hop)
        } else {
          _newOperatorInstance(classOf[GroupByPreShuffleOperator], hop)
        }
      }
      case hop: org.apache.hadoop.hive.ql.exec.PTFOperator => {
        _newOperatorInstance(classOf[PartitionTableFunctionOperator], hop)
      }
      case _ => throw new HiveException("Unsupported Hive operator: " + hiveOp.getClass.getName)
    }

    logDebug("Replacing %s with %s".format(hiveOp.getClass.getName, sharkOp.getClass.getName))
    sharkOp.asInstanceOf[Operator[T]]
  }

  private def _newOperatorInstance[T <: HiveDesc](
      cls: Class[_ <: Operator[T]], hiveOp: HOperator[T]): Operator[T] = {
    val op = cls.newInstance()
    op.setDesc(hiveOp.getConf())
    op
  }

  private def _createAndSetParents[T <: HiveDesc](op: Operator[T], 
      parents: Seq[HOperator[_ <: HiveDesc]]) = {
    if (parents != null) {
      parents foreach { parent =>
        _createOperatorTree(parent).addChild(op)
      }
    }
    op
  }

  /**
   * Given a terminal operator in Hive, create the plan that uses Shark physical
   * operators.
   */
  private def _createOperatorTree[T <: HiveDesc](hiveOp: HOperator[T]): Operator[T] = {
    val current = createSingleOperator(hiveOp)
    val parents = hiveOp.getParentOperators
    if (parents != null) {
      _createAndSetParents(current, parents.toSeq)
    }
    else {
      current
    }
  }

  private def _findHiveTerminalOperator(hiveOp: HOperator[_ <: HiveDesc]): HOperator[_ <: HiveDesc] = {
    if (hiveOp.getChildOperators() == null || hiveOp.getChildOperators().size() == 0) {
      hiveOp
    } else {
      hiveOp.getChildOperators().head
    }
  }

}

