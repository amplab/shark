package shark.exec

import org.apache.hadoop.hive.ql.exec.{LimitOperator => HiveLimitOperator}

import scala.collection.Iterator
import scala.reflect.BeanProperty

class LimitOperator extends UnaryOperator[HiveLimitOperator] {

  @BeanProperty var limit: Int = _
  
  override def initializeOnMaster() {
    limit = hiveOp.getConf().getLimit()
  }

  override def processPartition[T](iter: Iterator[T]) = {
    val limit = this.limit
    iter.take(limit)
  }

}

