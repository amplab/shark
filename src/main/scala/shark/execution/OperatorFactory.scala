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
import org.apache.hadoop.hive.ql.metadata.HiveException

import shark.LogHelper

import spark.storage.StorageLevel


/**
 * Given a Hive plan, OperatorFactory creates the corresponding Shark plan.
 */
object OperatorFactory extends LogHelper {

  /**
   * Given a Hive plan (that is any operator in the tree), create the plan that
   * uses Shark operators. This function automatically finds the Hive terminal
   * operator, and replicate the plan recursively up.
   */
  def createSharkPlan(hiveOp: HiveOperator): TerminalOperator = {
    val hiveTerminalOp = _findHiveTerminalOperator(hiveOp)
    _createOperatorTree(hiveTerminalOp).asInstanceOf[TerminalOperator]
  }

  def createSharkMemoryStoreOutputPlan(
      hiveTerminalOp: HiveOperator,
      tableName: String,
      storageLevel: StorageLevel,
      numColumns: Int,
      useTachyon: Boolean,
      useUnionRDD: Boolean): TerminalOperator = {
    val sinkOp = _newOperatorInstance(
      classOf[MemoryStoreSinkOperator], hiveTerminalOp).asInstanceOf[MemoryStoreSinkOperator]
    sinkOp.tableName = tableName
    sinkOp.storageLevel = storageLevel
    sinkOp.numColumns = numColumns
    sinkOp.useTachyon = useTachyon
    sinkOp.useUnionRDD = useUnionRDD
    _createAndSetParents(sinkOp, hiveTerminalOp.getParentOperators).asInstanceOf[TerminalOperator]
  }

  def createSharkFileOutputPlan(hiveTerminalOp: HiveOperator): TerminalOperator = {
    val sinkOp = _newOperatorInstance(classOf[FileSinkOperator], hiveTerminalOp)
    _createAndSetParents(
      sinkOp, hiveTerminalOp.getParentOperators).asInstanceOf[TerminalOperator]
  }

  def createSharkRddOutputPlan(hiveTerminalOp: HiveOperator): TerminalOperator = {
    val sinkOp = _newOperatorInstance(classOf[TableRddSinkOperator], hiveTerminalOp)
    _createAndSetParents(sinkOp, hiveTerminalOp.getParentOperators).asInstanceOf[TerminalOperator]
  }

  /** Create a Shark operator given the Hive operator. */
  private def createSingleOperator(hiveOp: HiveOperator): Operator[_] = {
    // This is kind of annoying, but it works with strong typing ...
    val sharkOp = hiveOp match {
      case hop: org.apache.hadoop.hive.ql.exec.TableScanOperator =>
        _newOperatorInstance(classOf[TableScanOperator], hiveOp)
      case hop: org.apache.hadoop.hive.ql.exec.SelectOperator =>
        _newOperatorInstance(classOf[SelectOperator], hiveOp)
      case hop: org.apache.hadoop.hive.ql.exec.FileSinkOperator =>
        _newOperatorInstance(classOf[TerminalOperator], hiveOp)
      case hop: org.apache.hadoop.hive.ql.exec.LimitOperator =>
        _newOperatorInstance(classOf[LimitOperator], hiveOp)
      case hop: org.apache.hadoop.hive.ql.exec.FilterOperator =>
        _newOperatorInstance(classOf[FilterOperator], hiveOp)
      case hop: org.apache.hadoop.hive.ql.exec.ReduceSinkOperator =>
        _newOperatorInstance(classOf[ReduceSinkOperator], hiveOp)
      case hop: org.apache.hadoop.hive.ql.exec.ExtractOperator =>
        _newOperatorInstance(classOf[ExtractOperator], hiveOp)
      case hop: org.apache.hadoop.hive.ql.exec.UnionOperator =>
        _newOperatorInstance(classOf[UnionOperator], hiveOp)
      case hop: org.apache.hadoop.hive.ql.exec.JoinOperator =>
        _newOperatorInstance(classOf[JoinOperator], hiveOp)
      case hop: org.apache.hadoop.hive.ql.exec.MapJoinOperator =>
        _newOperatorInstance(classOf[MapJoinOperator], hiveOp)
      case hop: org.apache.hadoop.hive.ql.exec.ScriptOperator =>
        _newOperatorInstance(classOf[ScriptOperator], hiveOp)
      case hop: org.apache.hadoop.hive.ql.exec.LateralViewForwardOperator =>
        _newOperatorInstance(classOf[LateralViewForwardOperator], hiveOp)
      case hop: org.apache.hadoop.hive.ql.exec.LateralViewJoinOperator =>
        _newOperatorInstance(classOf[LateralViewJoinOperator], hiveOp)
      case hop: org.apache.hadoop.hive.ql.exec.UDTFOperator =>
        _newOperatorInstance(classOf[UDTFOperator], hiveOp)
      case hop: org.apache.hadoop.hive.ql.exec.ForwardOperator =>
        _newOperatorInstance(classOf[ForwardOperator], hiveOp)
      case hop: org.apache.hadoop.hive.ql.exec.GroupByOperator => {
        // For GroupBy, we separate post shuffle from pre shuffle.
        if (GroupByOperator.isPostShuffle(hop)) {
          _newOperatorInstance(classOf[GroupByPostShuffleOperator], hiveOp)
        } else {
          _newOperatorInstance(classOf[GroupByPreShuffleOperator], hiveOp)
        }
      }
      case _ => throw new HiveException("Unsupported Hive operator: " + hiveOp.getClass.getName)
    }

    logDebug("Replacing %s with %s".format(hiveOp.getClass.getName, sharkOp.getClass.getName))
    sharkOp
  }

  private def _newOperatorInstance[T <: HiveOperator](
      cls: Class[_ <: Operator[T]], hiveOp: HiveOperator): Operator[_] = {
    val op = cls.newInstance()
    op.hiveOp = hiveOp.asInstanceOf[T]
    op
  }

  private def _createAndSetParents(op: Operator[_], parents: Seq[HiveOperator]) = {
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
  private def _createOperatorTree(hiveOp: HiveOperator): Operator[_] = {
    val current = createSingleOperator(hiveOp)
    val parents = hiveOp.getParentOperators
    if (parents != null) {
      _createAndSetParents(current, parents.toSeq)
    }
    else {
      current
    }
  }

  private def _findHiveTerminalOperator(hiveOp: HiveOperator): HiveOperator = {
    if (hiveOp.getChildOperators() == null || hiveOp.getChildOperators().size() == 0) {
      hiveOp
    } else {
      hiveOp.getChildOperators().head
    }
  }

}

