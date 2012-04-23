package shark.exec
import spark.RDD

import org.apache.hadoop.hive.ql.exec.{ForwardOperator => HiveForwardOperator}

class ForwardOperator extends UnaryOperator[HiveForwardOperator]
with Serializable {
  
  override def execute(): RDD[_] = executeParents().head._2

  override def processPartition[T](iter: Iterator[T]) = iter

}

