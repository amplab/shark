package shark.execution.optimization

import java.util.BitSet
import scala.collection.JavaConversions.{asScalaBuffer, 
  bufferAsJavaList, collectionAsScalaIterable} 
import scala.collection.mutable.{Set, HashSet}

import org.apache.hadoop.hive.ql.exec.GroupByPreShuffleOperator
import org.apache.hadoop.hive.ql.metadata.Table
import org.apache.hadoop.hive.ql.plan.SelectDesc

import shark.execution.{FilterOperator, JoinOperator, 
  MapJoinOperator, Operator, ReduceSinkOperator,
  SelectOperator, TopOperator}

import shark.memstore2.{ColumnarStruct, TablePartitionIterator}

class ColumnPruner(@transient op: TopOperator[_], @transient tbl: Table) extends Serializable {

  private val _columnsUsed = {
    val colsToKeep = computeColumnsToKeep()
    val allColumns = tbl.getAllCols().map(x => x.getName())
    var b = new BitSet()
    for (i <- Range(0, allColumns.size()) if (colsToKeep.contains(allColumns(i)))) {
      b.set(i, true)
    }
    b
  }

  def getColumnsUsed(): BitSet = {
    _columnsUsed
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
    op match {
      case selOp: SelectOperator => {
        val cnf = selOp.getConf
        //Select Descriptor contains SelectConf, which holds the list of columns
        //referenced by the select op
        if (cnf != null) {
          cols ++= (HashSet() ++ cnf.getColList).flatMap(_.getCols)
        }
      }
      case filterOp: FilterOperator => {
        val cnf = filterOp.getConf
        //FilterDesc has predicates, which are the columns involved in predicate operations
        cols ++= (HashSet() ++ cnf.getPredicate.getCols)
      }
      case joinOp: JoinOperator => {
        val cnf = parentOp.asInstanceOf[ReduceSinkOperator].getConf
        //before a regular join operation, the reduce sink operator is always present.
        //the key and value columns need to be examined for the input to the join
        cols ++= (HashSet() ++ cnf.getKeyCols ++ cnf.getValueCols).flatMap(_.getCols)
      }
      case joinOp: MapJoinOperator => {
        val cnf = joinOp.getConf
        val evals = (HashSet() ++ cnf.getKeys.values ++ cnf.getExprs.values)
        cols ++= evals.flatMap(x => x).flatMap(_.getCols)
      }
      case groupBy: GroupByPreShuffleOperator => {
        val keys = groupBy.getConf.getKeys
        cols ++= (HashSet() ++ keys).flatMap { k =>
          val c = k.getCols
          if (c == null) Seq() else c
        }
      }
      case _ =>
    }
    //recurse on the subtree
    op.childOperators.foreach(y => {
      computeColumnsToKeep(y, cols, op)
    })
  }
}