package shark.execution

import org.apache.hadoop.hive.ql.exec.{ForwardOperator => HiveForwardOperator}

import spark.RDD


class ForwardOperator extends UnaryOperator[HiveForwardOperator] {

  override def execute(): RDD[_] = executeParents().head._2

  override def processPartition(split: Int, iter: Iterator[_]) =
    throw new UnsupportedOperationException("ForwardOperator.processPartition()")

}
