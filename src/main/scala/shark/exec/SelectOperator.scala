package shark.exec

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
  
  @transient var eval: Array[ExprNodeEvaluator] = _
  
  override def initializeOnMaster() {
    conf = hiveOp.getConf()
  }
  
  override def initializeOnSlave() {
    if (!conf.isSelStarNoCompute) {
      eval = conf.getColList().map(ExprNodeEvaluatorFactory.get(_)).toArray
      eval.foreach(_.initialize(objectInspector))
    }
  }

  override def processPartition[T](iter: Iterator[T]) = {
    if (conf.isSelStarNoCompute) {
      iter
    } else {
      iter.map { row =>
        eval.map(x => x.evaluate(row))
      }
    }
  }

}

