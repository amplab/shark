package shark.exec

import org.apache.hadoop.hive.ql.metadata.HiveException

import scala.collection.JavaConversions._
import shark.LogHelper


/**
 * Given a Hive plan, OperatorFactory creates the corresponding Shark plan.
 */
object OperatorFactory extends LogHelper {
  
  /**
   * Given a Hive plan (that is any operator in the tree), create the plan that
   * uses Shark operators. This function automatically finds the Hive terminal
   * operator, and replicate the plan recursively up.
   */
  def createSharkPlan(hiveOp: HiveOperator): TerminalOperator = {
    val hiveTerminalOp = _findHiveTerminalOperator(hiveOp)
    _createOperatorTree(hiveTerminalOp).asInstanceOf[TerminalOperator]
  }

  def createSharkCacheOutputPlan(hiveTerminalOp: HiveOperator, tableName: String): TerminalOperator = {
    val terminalOp = _newOperatorInstance(classOf[CacheSinkOperator], hiveTerminalOp).asInstanceOf[CacheSinkOperator]
    terminalOp.tableName = tableName
    _createAndSetParents(terminalOp, hiveTerminalOp.getParentOperators).asInstanceOf[TerminalOperator]
  }
  
  def createSharkFileOutputPlan(hiveTerminalOp: HiveOperator): TerminalOperator = {
    val terminalOp = _newOperatorInstance(classOf[FileSinkOperator], hiveTerminalOp)
    _createAndSetParents(terminalOp, hiveTerminalOp.getParentOperators).asInstanceOf[TerminalOperator]
  }

  def createSharkRddOutputPlan(hiveTerminalOp: HiveOperator): TerminalOperator = {
    val terminalOp = _newOperatorInstance(classOf[TableRddSinkOperator], hiveTerminalOp)
    _createAndSetParents(terminalOp, hiveTerminalOp.getParentOperators).asInstanceOf[TerminalOperator]
  }
  
  /** Create a Shark operator given the Hive operator. */
  private def createSingleOperator(hiveOp: HiveOperator): Operator[_] = {
    // This is kind of annoying, but it works with strong typing ...
    val sharkOp = hiveOp match {
      case hop: org.apache.hadoop.hive.ql.exec.TableScanOperator =>
        _newOperatorInstance(classOf[TableScanOperator], hiveOp)
      case hop: org.apache.hadoop.hive.ql.exec.SelectOperator =>
        _newOperatorInstance(classOf[SelectOperator], hiveOp)
      case hop: org.apache.hadoop.hive.ql.exec.FileSinkOperator =>
        _newOperatorInstance(classOf[TerminalOperator], hiveOp)
      case hop: org.apache.hadoop.hive.ql.exec.LimitOperator =>
        _newOperatorInstance(classOf[LimitOperator], hiveOp)
      case hop: org.apache.hadoop.hive.ql.exec.FilterOperator =>
        _newOperatorInstance(classOf[FilterOperator], hiveOp)
      case hop: org.apache.hadoop.hive.ql.exec.ReduceSinkOperator =>
        _newOperatorInstance(classOf[ReduceSinkOperator], hiveOp)
      case hop: org.apache.hadoop.hive.ql.exec.ExtractOperator =>
        _newOperatorInstance(classOf[ExtractOperator], hiveOp)
      case hop: org.apache.hadoop.hive.ql.exec.UnionOperator =>
        _newOperatorInstance(classOf[UnionOperator], hiveOp)
      case hop: org.apache.hadoop.hive.ql.exec.JoinOperator =>
        _newOperatorInstance(classOf[JoinOperator], hiveOp)
      case hop: org.apache.hadoop.hive.ql.exec.MapJoinOperator =>
        _newOperatorInstance(classOf[MapJoinOperator], hiveOp)
      case hop: org.apache.hadoop.hive.ql.exec.ScriptOperator =>
        _newOperatorInstance(classOf[ScriptOperator], hiveOp)
      case hop: org.apache.hadoop.hive.ql.exec.LateralViewForwardOperator => 
        _newOperatorInstance(classOf[LateralViewForwardOperator], hiveOp)
      case hop: org.apache.hadoop.hive.ql.exec.LateralViewJoinOperator => 
        _newOperatorInstance(classOf[LateralViewJoinOperator], hiveOp)
      case hop: org.apache.hadoop.hive.ql.exec.UDTFOperator => 
        _newOperatorInstance(classOf[UDTFOperator], hiveOp)
      case hop: org.apache.hadoop.hive.ql.exec.ForwardOperator => 
        _newOperatorInstance(classOf[ForwardOperator], hiveOp)
      case hop: org.apache.hadoop.hive.ql.exec.GroupByOperator => {
        // For GroupBy, we separate post shuffle from pre shuffle.
        if (GroupByOperator.isPostShuffle(hop)) {
          _newOperatorInstance(classOf[GroupByPostShuffleOperator], hiveOp)
        } else {
          _newOperatorInstance(classOf[GroupByPreShuffleOperator], hiveOp)
        }
      }
      case _ => throw new HiveException("Unsupported Hive operator: " + hiveOp.getClass.getName)
    }
    
    logInfo("Replacing %s with %s".format(hiveOp.getClass.getName, sharkOp.getClass.getName))
    sharkOp
  }
  
  private def _newOperatorInstance[T <: HiveOperator](
      cls: Class[_ <: Operator[T]], hiveOp: HiveOperator): Operator[_] = {
    val op = cls.newInstance()
    op.hiveOp = hiveOp.asInstanceOf[T]
    op
  }

  private def _createAndSetParents(op: Operator[_], parents: Seq[HiveOperator]) = {
    if (parents != null) {
      parents foreach { parent =>
        _createOperatorTree(parent).addChild(op)
      }
    }
    op
  }
  
  /**
   * Given a terminal operator in Hive, create the plan that uses Shark physical
   * operators.
   */
  private def _createOperatorTree(hiveOp: HiveOperator): Operator[_] = {
    val current = createSingleOperator(hiveOp)
    val parents = hiveOp.getParentOperators
    if (parents != null) {
      _createAndSetParents(current, parents.toSeq)
    }
    else {
      current
    }
  }
  
  private def _findHiveTerminalOperator(hiveOp: HiveOperator): HiveOperator = {
    if (hiveOp.getChildOperators() == null || hiveOp.getChildOperators().size() == 0) {
      hiveOp
    } else {
      hiveOp.getChildOperators().head
    }
  }

}

