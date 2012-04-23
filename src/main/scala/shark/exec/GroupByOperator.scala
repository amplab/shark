package shark.exec

import org.apache.hadoop.hive.ql.exec.{GroupByOperator => HiveGroupByOperator}
import org.apache.hadoop.hive.ql.exec.{ReduceSinkOperator => HiveReduceSinkOperator}
import org.apache.hadoop.hive.ql.plan.GroupByDesc


object GroupByOperator {
  
  def isPostShuffle(op: HiveGroupByOperator): Boolean = {
    /*return (op.getConf.getMode == GroupByDesc.Mode.FINAL ||
            op.getConf.getMode == GroupByDesc.Mode.MERGEPARTIAL ||
            op.getConf.getMode == GroupByDesc.Mode.COMPLETE)*/
    op.getParentOperators().get(0).isInstanceOf[HiveReduceSinkOperator]
  }
  
}

