package shark.execution

import org.apache.hadoop.hive.ql.exec.{ExprNodeEvaluator, ExprNodeEvaluatorFactory}
import org.apache.hadoop.hive.ql.exec.{SelectOperator => HiveSelectOperator}
import org.apache.hadoop.hive.ql.plan.SelectDesc
import scala.collection.JavaConversions._
import scala.reflect.BeanProperty


/**
 * An operator that does projection, i.e. selecting certain columns and
 * filtering out others.
 */
class SelectOperator extends UnaryOperator[HiveSelectOperator] {

  @BeanProperty var conf: SelectDesc = _
  
  @transient var evals: Array[ExprNodeEvaluator] = _
  
  override def initializeOnMaster() {
    conf = hiveOp.getConf()
  }
  
  override def initializeOnSlave() {
    if (!conf.isSelStarNoCompute) {
      evals = conf.getColList().map(ExprNodeEvaluatorFactory.get(_)).toArray
      evals.foreach(_.initialize(objectInspector))
    }
  }

  override def processPartition[T](iter: Iterator[T]) = {
    if (conf.isSelStarNoCompute) {
      iter
    } else {
      val reusedRow = new Array[Object](evals.length)
      iter.map { row =>
        var i = 0
        while (i < evals.length) {
          reusedRow(i) = evals(i).evaluate(row)
          i += 1
        }
        reusedRow
      }
    }
  }

}

