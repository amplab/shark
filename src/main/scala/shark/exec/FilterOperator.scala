package shark.exec

import org.apache.hadoop.hive.ql.exec.{ExprNodeEvaluator, ExprNodeEvaluatorFactory}
import org.apache.hadoop.hive.ql.exec.{FilterOperator => HiveFilterOperator}
import org.apache.hadoop.hive.ql.metadata.HiveException
import org.apache.hadoop.hive.ql.plan.FilterDesc
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector

import scala.collection.Iterator
import scala.reflect.BeanProperty


class FilterOperator extends UnaryOperator[HiveFilterOperator] with Serializable {
  
  @transient var conditionEvaluator: ExprNodeEvaluator = _

  @BeanProperty var conf: FilterDesc = _

  override def initializeOnMaster() {
    conf = hiveOp.getConf()
  }

  override def initializeOnSlave() {
    try {
      conditionEvaluator = ExprNodeEvaluatorFactory.get(conf.getPredicate())
    } catch {
      case e: Throwable => throw new HiveException(e)
    }
  }

  override def processPartition[T](iter: Iterator[T]) = {
    val conditionInspector = conditionEvaluator.initialize(objectInspector)
      .asInstanceOf[PrimitiveObjectInspector]

    iter.filter { row =>
      java.lang.Boolean.TRUE.equals(
        conditionInspector.getPrimitiveJavaObject(conditionEvaluator.evaluate(row)))
    }
  }

}