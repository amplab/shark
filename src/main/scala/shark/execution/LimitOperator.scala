package shark.execution

import scala.collection.Iterator
import scala.reflect.BeanProperty

import org.apache.hadoop.hive.ql.exec.{LimitOperator => HiveLimitOperator}


class LimitOperator extends UnaryOperator[HiveLimitOperator] {

  @BeanProperty var limit: Int = _

  override def initializeOnMaster() {
    limit = hiveOp.getConf().getLimit()
  }

  override def processPartition(split: Int, iter: Iterator[_]) = iter.take(limit)
}

