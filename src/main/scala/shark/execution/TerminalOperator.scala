package shark.execution

import java.util.Date

import org.apache.hadoop.io.Text
import org.apache.hadoop.io.BytesWritable
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.exec.{FileSinkOperator => HiveFileSinkOperator, JobCloseFeedBack}
import org.apache.hadoop.hive.serde2.Serializer
import org.apache.hadoop.mapred.{TaskID, TaskAttemptID, HadoopWriter}

import scala.reflect.BeanProperty

import shark.{CacheKey, RDDUtils, SharkEnv}
import spark.{RDD, TaskContext}

import java.util.Date

/**
 * File sink operator. It can accomplish one of the three things:
 * - write query output to disk
 * - cache query output
 * - return query as RDD directly (without materializing it)
 */
class TerminalOperator extends TerminalAbstractOperator[HiveFileSinkOperator] {

  // Create a local copy of hconf and hiveSinkOp so we can XML serialize it.
  @BeanProperty var localHiveOp: HiveFileSinkOperator = _
  @BeanProperty var localHconf: HiveConf = _
  @BeanProperty val now = new Date()

  override def initializeOnMaster() {
    localHconf = super.hconf
    // Set parent to null so we won't serialize the entire query plan.
    hiveOp.setParentOperators(null)
    hiveOp.setChildOperators(null)
    hiveOp.setInputObjInspectors(null)
    localHiveOp = hiveOp
  }

  override def initializeOnSlave() {
    localHiveOp.initialize(localHconf, Array(objectInspector))
  }

  override def processPartition[T](iter: Iterator[T]): Iterator[_] = iter

}


class FileSinkOperator extends TerminalOperator with Serializable {

  def initializeOnSlave(context: TaskContext) {
    setConfParams(localHconf, context)
    initializeOnSlave()
  }

  def setConfParams(conf: HiveConf, context: TaskContext) {
    val jobID = context.stageId
    val splitID = context.splitId 
    val jID = HadoopWriter.createJobID(now, jobID)
    val taID = new TaskAttemptID(new TaskID(jID, true, splitID), 0)
    conf.set("mapred.job.id", jID.toString)
    conf.set("mapred.tip.id", taID.getTaskID.toString) 
    conf.set("mapred.task.id", taID.toString)
    conf.setBoolean("mapred.task.is.map", true)
    conf.setInt("mapred.task.partition", splitID)
  }

  override def processPartition[T](iter: Iterator[T]): Iterator[_] = {
    iter.foreach { row =>
      localHiveOp.processOp(row, 0)
    }
    localHiveOp.closeOp(false)
    iter
  }

  override def execute(): RDD[_] = {
    val inputRdd = if (parentOperators.size == 1) executeParents().head._2 else null
    val rddPreprocessed = preprocessRdd(inputRdd)
    rddPreprocessed.context.runJob(
      rddPreprocessed, FileSinkOperator.executeProcessFileSinkPartition(this))
    hiveOp.jobClose(localHconf, true, new JobCloseFeedBack)
    rddPreprocessed
  }
}


object FileSinkOperator {
  def executeProcessFileSinkPartition(operator: FileSinkOperator) = {
    val op = OperatorSerializationWrapper(operator)
    def writeFiles(context: TaskContext, iter: Iterator[_]): Boolean = {
      op.logDebug("Started executing mapPartitions for operator: " + op)
      op.logDebug("Input object inspectors: " + op.objectInspectors)

      op.initializeOnSlave(context)
      val newPart = op.processPartition(iter)
      op.logDebug("Finished executing mapPartitions for operator: " + op)

      true
    }
    writeFiles _
  }
}


/**
 * Cache the RDD and force evaluate it (so the cache is filled). We avoid
 * using anonymous class here for easier debugging ...
 */
class CacheSinkOperator(@BeanProperty var tableName: String) extends TerminalOperator {

  def this() = this(null)
 
  override def processPartition[T](iter: Iterator[T]): Iterator[_] = {
    RDDUtils.serialize(
        iter,
        localHconf,
        localHiveOp.getConf.getTableInfo,
        objectInspector)
  }

  override def postprocessRdd[T](rdd: RDD[T]): RDD[_] = {
    SharkEnv.cache.put(new CacheKey(tableName), rdd)
    rdd.foreach(_ => Unit)
    rdd
  }
}


/**
 * Collect the output as a TableRDD.
 */
class TableRddSinkOperator extends TerminalOperator {}

