package shark.execution

import java.util.{Date, HashMap => JavaHashMap}

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.BytesWritable
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.exec.{FileSinkOperator => HiveFileSinkOperator, JobCloseFeedBack}
import org.apache.hadoop.hive.serde2.Serializer
import org.apache.hadoop.mapred.{TaskID, TaskAttemptID, HadoopWriter}

import scala.reflect.BeanProperty

import shark.{CacheKey, RDDUtils, SharkConfVars, SharkEnv}
import spark.{RDD, TaskContext}


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

  // Pass the file extension ConfVar used by HiveFileSinkOperator.
  @BeanProperty var outputFileExtension: String = _

  override def initializeOnMaster() {
    super.initializeOnMaster()
    outputFileExtension = HiveConf.getVar(localHconf, HiveConf.ConfVars.OUTPUT_FILE_EXTENSION)
  }

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

    // Variables used by FileSinkOperator.
    if (outputFileExtension != null) {
      conf.setVar(HiveConf.ConfVars.OUTPUT_FILE_EXTENSION, outputFileExtension)
    }
  }

  override def processPartition[T](iter: Iterator[T]): Iterator[_] = {
    iter.foreach { row =>
      localHiveOp.processOp(row, 0)
    }

    // Create missing parent directories so that the HiveFileSinkOperator can rename
    // temp file without complaining.

    // Two rounds of reflection are needed, since the FSPaths reference is private, and
    // the FSPaths' finalPaths reference isn't publicly accessible.
    val fspField = localHiveOp.getClass.getDeclaredField("fsp")
    fspField.setAccessible(true)
    val fileSystemPaths = fspField.get(localHiveOp).asInstanceOf[HiveFileSinkOperator#FSPaths]

    // File paths for dynamic partitioning are determined separately. See FileSinkOperator.java.
    if (fileSystemPaths != null) {
      val finalPathsField = fileSystemPaths.getClass.getDeclaredField("finalPaths")
      finalPathsField.setAccessible(true)
      val finalPaths = finalPathsField.get(fileSystemPaths).asInstanceOf[Array[Path]]

      // Get a reference to the FileSystem. No need for reflection here.
      val fileSystem = FileSystem.get(localHconf)

      for (idx <- 0 until finalPaths.length) {
        var finalPath = finalPaths(idx)
        if (finalPath == null) {
          // If a query results in no output rows, then file paths for renaming will be
          // created in localHiveOp.closeOp instead of processOp. But we need them before
          // that to check for missing parent directories.
          val createFilesMethod = localHiveOp.getClass.getDeclaredMethod(
            "createBucketFiles", classOf[HiveFileSinkOperator#FSPaths])
          createFilesMethod.setAccessible(true)
          createFilesMethod.invoke(localHiveOp, fileSystemPaths)
          finalPath = finalPaths(idx)
        }
        if (!fileSystem.exists(finalPath.getParent())) fileSystem.mkdirs(finalPath.getParent())
      }
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

  @BeanProperty var initialColumnSize: Int = _

  def this() = this(null)

  override def initializeOnMaster() {
    super.initializeOnMaster()
    initialColumnSize = SharkConfVars.getIntVar(localHconf, SharkConfVars.COLUMN_INITIALSIZE)
  }

  override def initializeOnSlave() {
    super.initializeOnSlave()
    localHconf.setInt(SharkConfVars.COLUMN_INITIALSIZE.varname, initialColumnSize)
  }

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
