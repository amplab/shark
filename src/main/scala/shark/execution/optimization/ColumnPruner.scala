package shark.execution.optimization

import scala.collection.IndexedSeqLike
import scala.collection.JavaConversions._
import org.apache.hadoop.hive.ql.exec.GroupByPreShuffleOperator
import org.apache.hadoop.hive.ql.exec.ExprNodeColumnEvaluator
import org.apache.hadoop.hive.ql.metadata.Table
import org.apache.hadoop.hive.ql.plan.SelectDesc
import shark.execution.FilterOperator
import shark.execution.JoinOperator
import shark.execution.MapJoinOperator
import shark.execution.Operator
import shark.execution.ReduceSinkOperator
import shark.execution.SelectOperator
import shark.execution.TopOperator
import shark.memstore2.ColumnarStruct
import shark.memstore2.TablePartitionIterator
import java.util.BitSet

class ColumnPruner(@transient op: TopOperator[_], @transient tbl: Table) extends Serializable{

  val tuple = computeColumnsToKeep
  val skip = tuple._1 && tuple._2.isEmpty
  val columnsUsed = {
    val allColumns = tbl.getAllCols().map(x => x.getName())
    val b = new BitSet(allColumns.size)
    for (i <- Range(0, allColumns.size()) if (tuple._2.contains(allColumns(i)))) {
      b.set(i)
    }
    b
  }

  def prune(iterator: Iterator[ColumnarStruct]): Iterator[ColumnarStruct] = {
    val delegate = iterator.asInstanceOf[TablePartitionIterator]
    new TablePartitionIterator(delegate.numRows,delegate.columnIterators)
  }
  
  private def computeColumnsToKeep(): Tuple2[Boolean, Seq[String]] = {
    val cols = scala.collection.mutable.ArrayBuffer[String]()
    val selStar = ColumnPruner.computeColumnsToKeep(op, cols)
    (selStar, cols)
  }
}

object ColumnPruner {
  
  def computeColumnsToKeep(op: Operator[_], cols: scala.collection.mutable.ArrayBuffer[String], parentOp: Operator[_] = null): Boolean = {
    val selStar = if (op.isInstanceOf[SelectOperator]) {
      val selOp = op.asInstanceOf[SelectOperator]
      val cnf = selOp.getConf
      Option[SelectDesc](cnf) match {
        case Some(_) => {
          cols ++= cnf.getColList
            .flatMap(x => x.getCols)

          cnf.isSelectStar()
        }
        case None => true
      }
    } else if (op.isInstanceOf[FilterOperator]){
      val filterOp = op.asInstanceOf[FilterOperator]
      val cnf = filterOp.getConf
      cols ++= cnf.getPredicate.getCols
      
      false
    } else if (op.isInstanceOf[JoinOperator]) {
      val joinOp = op.asInstanceOf[JoinOperator]
      val cnf = parentOp.asInstanceOf[ReduceSinkOperator].getConf
      cols ++= (List() ++ cnf.getKeyCols ++ cnf.getValueCols)
      .flatMap(e =>e.getCols)
      true
    } else if (op.isInstanceOf[MapJoinOperator]) {
      val joinOp = op.asInstanceOf[MapJoinOperator]
      val cnf = joinOp.getConf
      cols ++= (List() ++ cnf.getKeys.values ++ cnf.getExprs.values)
       .flatMap(x =>x)
       .flatMap(e => e.getCols)
      true
    } else if (op.isInstanceOf[GroupByPreShuffleOperator]){
      val groupBy = op.asInstanceOf[GroupByPreShuffleOperator]
      cols ++= groupBy.getConf.getKeys.flatMap(k => k.getCols)
      true
    } else {
      true
    }
     op.childOperators.foldLeft(selStar)((x, y) => {
       val s = computeColumnsToKeep(y, cols, op)
       s && x
     })
  }
}