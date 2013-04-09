package shark.execution.optimizer

import scala.collection.IndexedSeqLike
import scala.collection.JavaConversions._
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
import shark.memstore.ColumnarStruct
import shark.memstore.PrunedTableIterator
import shark.memstore.TableIterator
import org.apache.hadoop.hive.ql.exec.GroupByPreShuffleOperator

class ColumnPruner(op: TopOperator[_], tbl: Table) extends Serializable{

  val tuple = computeColumnsToKeep
  val skip = tuple._1 && tuple._2.isEmpty
  val outputColumnIndices = {
    val allColumns = tbl.getAllCols().map(x => x.getName())
    for (i <- Range(0, allColumns.size()) if (tuple._2.contains(allColumns(i)))) 
      yield (i)
  }

  def prune(iterator: Iterator[ColumnarStruct]): Iterator[ColumnarStruct] = {
    new PrunedTableIterator(skip, outputColumnIndices, iterator.asInstanceOf[TableIterator])
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
      cols ++= (cnf.getKeyCols ++ cnf.getValueCols)
      .flatMap(e =>e.getCols)
      true
    } else if (op.isInstanceOf[MapJoinOperator]) {
      val joinOp = op.asInstanceOf[MapJoinOperator]
      val cnf = joinOp.getConf
      cols ++= (cnf.getKeys.values ++ cnf.getExprs.values)
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