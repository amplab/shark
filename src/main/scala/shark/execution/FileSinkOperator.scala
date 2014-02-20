/*
 * Copyright (C) 2012 The Regents of The University California.
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package shark.execution

import java.text.SimpleDateFormat
import java.util.Date

import scala.reflect.BeanProperty

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.exec.{FileSinkOperator => HiveFileSinkOperator}
import org.apache.hadoop.hive.ql.exec.JobCloseFeedBack
import org.apache.hadoop.hive.shims.ShimLoader
import org.apache.hadoop.mapred.{JobID, TaskAttemptID, TaskID}

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD

import shark.execution.serialization.OperatorSerializationWrapper


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
    def createJobID(time: Date, id: Int): JobID = {
      val formatter = new SimpleDateFormat("yyyyMMddHHmm")
      val jobtrackerID = formatter.format(new Date())
      return new JobID(jobtrackerID, id)
    }

    val jobID = context.stageId
    val splitID = context.partitionId
    val jID = createJobID(now, jobID)
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

  override def processPartition(split: Int, iter: Iterator[_]): Iterator[_] = {

    var numRows = 0

    iter.foreach { row =>
      numRows += 1
      // Process and writes each row to a temp file.
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
        if (!fileSystem.exists(finalPath.getParent)) {
          fileSystem.mkdirs(finalPath.getParent)
        }
      }
    }

    localHiveOp.closeOp(false /* abort */)
    Iterator(numRows)
  }

  override def execute(): RDD[_] = {
    val inputRdd = if (parentOperators.size == 1) executeParents().head._2 else null
    val rdd = preprocessRdd(inputRdd)

    parentOperators.head match {
      case op: LimitOperator =>
        // If there is a limit operator, let's run two partitions first. Once we finished running
        // the first two partitions, we use that to estimate how many more partitions we need to
        // run to satisfy the limit.

        val limit = op.limit
        val totalParts = rdd.partitions.length
        var rowsFetched = 0L
        var partsFetched = 0
        while (rowsFetched < limit && partsFetched < totalParts) {
          // The number of partitions to try in this iteration. It is ok for this number to be
          // greater than totalParts because we actually cap it at totalParts in runJob.
          var numPartsToTry = 2
          if (partsFetched > 0) {
            // If we didn't find any rows after the first iteration, just try all partitions next.
            // Otherwise, interpolate the number of partitions we need to try, but overestimate it
            // by 50%.
            if (rowsFetched == 0) {
              numPartsToTry = totalParts - 2
            } else {
              numPartsToTry = (1.5 * limit * partsFetched / rowsFetched).toInt
            }
          }
          numPartsToTry = math.max(0, numPartsToTry)  // guard against negative num of partitions

          rowsFetched += rdd.context.runJob(
            rdd,
            FileSinkOperator.executeProcessFileSinkPartition(this),
            partsFetched until math.min(partsFetched + numPartsToTry, totalParts),
            allowLocal = false).sum
          partsFetched += numPartsToTry
        }

      case _ =>
        val rows: Array[Long] = rdd.context.runJob(
          rdd, FileSinkOperator.executeProcessFileSinkPartition(this))
        logDebug("Total number of rows written: " + rows.sum)
    }

    localHiveOp.jobClose(localHconf, true /* success */, new JobCloseFeedBack)
    rdd
  }
}


object FileSinkOperator {
  // Write each partition's output to HDFS, and return the number of rows written.
  def executeProcessFileSinkPartition(operator: FileSinkOperator) = {
    val op = OperatorSerializationWrapper(operator)
    def writeFiles(context: TaskContext, iter: Iterator[_]): Long = {
      op.logDebug("Started executing mapPartitions for operator: " + op)
      op.logDebug("Input object inspectors: " + op.objectInspectors)

      op.initializeOnSlave(context)
      val numRows = op.processPartition(-1, iter).next().asInstanceOf[Int]
      op.logDebug("Finished executing mapPartitions for operator: " + op)
      numRows
    }
    writeFiles _
  }
}
