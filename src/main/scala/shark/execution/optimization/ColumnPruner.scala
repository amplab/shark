package shark.execution.optimization

import java.util.BitSet
import java.util.{List => JList}

import scala.collection.JavaConversions.{asScalaBuffer, bufferAsJavaList, collectionAsScalaIterable}
import scala.collection.mutable.{Set, HashSet}

import org.apache.hadoop.hive.ql.exec.GroupByPreShuffleOperator
import org.apache.hadoop.hive.ql.metadata.Table
import org.apache.hadoop.hive.ql.plan.SelectDesc
import org.apache.hadoop.hive.ql.plan.{FilterDesc, MapJoinDesc, ReduceSinkDesc}

import shark.execution.{FilterOperator, JoinOperator,
  MapJoinOperator, Operator, ReduceSinkOperator,
  SelectOperator, TopOperator}
import shark.memstore2.{ColumnarStruct, TablePartitionIterator}


class ColumnPruner(@transient op: TopOperator[_], @transient tbl: Table) extends Serializable {

  val columnsUsed: BitSet = {
    val colsToKeep = computeColumnsToKeep()
    val allColumns = tbl.getAllCols().map(x => x.getName())
    var b = new BitSet()
    for (i <- Range(0, allColumns.size()) if (colsToKeep.contains(allColumns(i)))) {
      b.set(i, true)
    }
    b
  }

  private def computeColumnsToKeep(): Set[String] = {
    val cols = HashSet[String]()
    computeColumnsToKeep(op, cols)
    cols
  }

  /**
   * Computes the column names that are referenced in the Query
   */
  private def computeColumnsToKeep(op: Operator[_],
    cols: HashSet[String], parentOp: Operator[_] = null): Unit = {
    def nullGuard[T](s: JList[T]): Seq[T] = {
      if (s == null) Seq[T]() else s
    }
    op match {
      case selOp: SelectOperator => {
        val cnf:SelectDesc = selOp.getConf
        //Select Descriptor contains SelectConf, which holds the list of columns
        //referenced by the select op
        if (cnf != null) {
          val evals = nullGuard(cnf.getColList)
          cols ++= (HashSet() ++ evals).flatMap(x => nullGuard(x.getCols))
        }
      }
      case filterOp: FilterOperator => {
        val cnf:FilterDesc = filterOp.getConf
        //FilterDesc has predicates, which are the columns involved in predicate operations
        if (cnf != null) {
          cols ++= (HashSet() ++ nullGuard(cnf.getPredicate.getCols))
        }
      }
      case joinOp: JoinOperator => {
        val cnf:ReduceSinkDesc = parentOp.asInstanceOf[ReduceSinkOperator].getConf
        //before a regular join operation, the reduce sink operator is always present.
        //the key and value columns need to be examined for the input to the join
        if (cnf != null) {
          val keyEvals = nullGuard(cnf.getKeyCols)
          val valEvals = nullGuard(cnf.getValueCols)
          val evals = (HashSet() ++ keyEvals ++ valEvals)
          cols ++= evals.flatMap(x => nullGuard(x.getCols))
        }
      }
      case joinOp: MapJoinOperator => {
        val cnf:MapJoinDesc = joinOp.getConf
        if (cnf != null) {
          val keyEvals = cnf.getKeys.values
          val valEvals = cnf.getExprs.values
          val evals = (HashSet() ++ keyEvals ++ valEvals)
          cols ++= evals.flatMap(x => x).flatMap(x => nullGuard(x.getCols))
        }
      }
      case groupBy: GroupByPreShuffleOperator => {
        val cnf = groupBy.getConf
        if (cnf != null) {
          val keys = nullGuard(groupBy.getConf.getKeys)
          cols ++= (HashSet() ++ keys).flatMap(x => nullGuard(x.getCols))
        }
      }
      case _ =>
    }
    //recurse on the subtree
    op.childOperators.foreach { y =>
      computeColumnsToKeep(y, cols, op)
    }
  }
}
