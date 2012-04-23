package shark.exec
import spark.RDD

import org.apache.hadoop.hive.ql.exec.{LateralViewForwardOperator => HiveLateralViewForwardOperator}

class LateralViewForwardOperator extends UnaryOperator[HiveLateralViewForwardOperator]
with Serializable {
  
  override def execute(): RDD[_] = executeParents().head._2

  override def processPartition[T](iter: Iterator[T]) = iter

}

