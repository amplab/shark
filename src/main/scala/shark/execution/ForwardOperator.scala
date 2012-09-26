package shark.execution

import org.apache.hadoop.hive.ql.exec.{ForwardOperator => HiveForwardOperator}

import spark.{RDD, Split}


class ForwardOperator extends UnaryOperator[HiveForwardOperator] {

  override def execute(): RDD[_] = executeParents().head._2

  override def processPartition(split: Split, iter: Iterator[_]) = iter

}
