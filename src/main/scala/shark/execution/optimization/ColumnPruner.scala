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

package shark.execution.optimization

import java.util.BitSet
import java.util.{List => JList}

import scala.collection.JavaConversions.{asScalaBuffer, collectionAsScalaIterable}
import scala.collection.mutable.{Set, HashSet}

import org.apache.hadoop.hive.ql.exec.GroupByPreShuffleOperator
import org.apache.hadoop.hive.ql.metadata.Table
import org.apache.hadoop.hive.ql.plan._

import shark.execution._


class ColumnPruner(@transient op: TopOperator[_], @transient tbl: Table) extends Serializable {

  val columnsUsed: BitSet = {
    val colsToKeep = computeColumnsToKeep()
    val allColumns = tbl.getCols().map(x => x.getName())

    if (colsToKeep.contains("*")) {
      // If colsToKeep contains a select *, use all columns.
      val b = new BitSet(allColumns.size)
      b.set(0, allColumns.size, true)
      b
    } else {
      // No need to prune partition columns - Hive does that for us.
      val b = new BitSet
      for (i <- Range(0, allColumns.size) if colsToKeep.contains(allColumns(i))) {
        b.set(i, true)
      }
      b
    }
  }

  private def computeColumnsToKeep(): Set[String] = {
    val cols = HashSet[String]()
    computeColumnsToKeep(op, cols)
    cols
  }

  /**
   * Computes the column names that are referenced in the Query
   */
  private def computeColumnsToKeep(
      op: Operator[_],
      cols: HashSet[String],
      parentOp: Operator[_] = null) {

    println("operator to check is " + op)

    op match {
      case selOp: SelectOperator =>
        cols ++= getColumnsFromSelectOp(selOp)

      case filterOp: FilterOperator =>
        val cnf: FilterDesc = filterOp.getConf
        //FilterDesc has predicates, which are the columns involved in predicate operations
        if (cnf != null) {
          cols ++= (HashSet() ++ nullGuard(cnf.getPredicate.getCols))
        }

      case joinOp: JoinOperator =>
        val cnf: ReduceSinkDesc = parentOp.asInstanceOf[ReduceSinkOperator].getConf
        //before a regular join operation, the reduce sink operator is always present.
        //the key and value columns need to be examined for the input to the join
        if (cnf != null) {
          val keyEvals = nullGuard(cnf.getKeyCols)
          val valEvals = nullGuard(cnf.getValueCols)
          val evals = HashSet() ++ keyEvals ++ valEvals
          cols ++= evals.flatMap(x => nullGuard(x.getCols))
        }

      case joinOp: MapJoinOperator =>
        val cnf: MapJoinDesc = joinOp.getConf
        if (cnf != null) {
          val keyEvals = cnf.getKeys.values
          val valEvals = cnf.getExprs.values
          val evals = HashSet() ++ keyEvals ++ valEvals
          cols ++= evals.flatMap(x => x).flatMap(x => nullGuard(x.getCols))
        }

      case groupBy: GroupByPreShuffleOperator =>
        val cnf: GroupByDesc = groupBy.getConf
        if (cnf != null) {
          val keys = nullGuard(groupBy.getConf.getKeys)
          cols ++= (HashSet() ++ keys).flatMap(x => nullGuard(x.getCols))
        }

      case lvj: LateralViewJoinOperator =>
        lvj.parentOperators.head match {
          case selOp: SelectOperator => cols ++= getColumnsFromSelectOp(selOp)
          case _ => // Do nothing
        }

      case _ =>
    }

    // recurse on the subtree
    val numChildren = op.childOperators.size
    var currentChildIndex = 0
    while (currentChildIndex < numChildren) {
      val childOp = op.childOperators(currentChildIndex)
      if (op.isInstanceOf[TableScanOperator] && childOp.isInstanceOf[LateralViewForwardOperator]) {
        // The query has a LATERAL VIEW command and its operator tree includes am LVJ Op.
        // See LateralViewJoinOperator.scala for documentation on execution details.
        // There is an implied SELECT * projection on a table's rows when we evaluate the LVF Op
        // from the UDTF Op branch, so short-circuit the pruning here.
        // Note that the actual Select Op in that branch only contains the Array evaluators, so we
        // can't column prune based on it.
        cols += "*"
      } else {
        computeColumnsToKeep(childOp, cols, op)
      }
      currentChildIndex = currentChildIndex + 1
    }
  }

  private def nullGuard[T](s: JList[T]): Seq[T] = {
    if (s == null) Seq[T]() else s
  }

  private def getColumnsFromSelectOp(selOp: SelectOperator): Set[String] = {
    val cnf: SelectDesc = selOp.getConf
    // Select Descriptor contains SelectConf, which holds the list of columns
    // referenced by the select op
    if (cnf != null) {
      if (cnf.isSelStarNoCompute) {
        // For star, return immediately since there is no point doing any further pruning.
        Set("*")
      } else {
        val evals = nullGuard(cnf.getColList)
        Set() ++ evals.flatMap(x => nullGuard(x.getCols))
      }
    } else {
      Set.empty
    }
  }
}
