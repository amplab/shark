package shark.execution

import org.apache.hadoop.hive.ql.exec.{LateralViewForwardOperator => HiveLateralViewForwardOperator}

import spark.{RDD, Split}

class LateralViewForwardOperator extends UnaryOperator[HiveLateralViewForwardOperator] {

  override def execute(): RDD[_] = executeParents().head._2

  override def processPartition(split: Split, iter: Iterator[_]) = iter

}

