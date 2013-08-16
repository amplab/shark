package shark.execution.optimization

import java.util.BitSet
import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.JavaConversions.bufferAsJavaList
import scala.collection.JavaConversions.collectionAsScalaIterable

import org.apache.hadoop.hive.ql.exec.GroupByPreShuffleOperator
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

class ColumnPruner(@transient op: TopOperator[_], @transient tbl: Table) extends Serializable{
  
  val tuple = computeColumnsToKeep
  val skip = tuple._1 && tuple._2.isEmpty
  val columnsUsed = {
    val allColumns = tbl.getAllCols().map(x => x.getName())
    var b = new BitSet()
    for (i <- Range(0, allColumns.size()) if (tuple._2.contains(allColumns(i)))) {
      b.set(i, true)
    }
    b
  }

  def getColumnsUsed() : BitSet = {
    columnsUsed
  }

  private def computeColumnsToKeep(): Tuple2[Boolean, Seq[String]] = {
    val cols = scala.collection.mutable.ArrayBuffer[String]()
    val selStar = computeColumnsToKeep(op, cols)
    (selStar, cols)
  }
  
  private def computeColumnsToKeep(op: Operator[_], cols: scala.collection.mutable.ArrayBuffer[String], parentOp: Operator[_] = null): Boolean = {
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
      val keys = groupBy.getConf.getKeys
      cols ++= keys.flatMap { k =>
        val c = k.getCols
        if (c == null) Seq() else c
      }
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
