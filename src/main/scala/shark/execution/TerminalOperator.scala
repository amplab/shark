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

import java.util.Date

import scala.collection.Map
import scala.collection.mutable.ArrayBuffer
import scala.reflect.BeanProperty

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{BytesWritable, Text, Writable}
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.exec.{FileSinkOperator => HiveFileSinkOperator, JobCloseFeedBack}
import org.apache.hadoop.hive.serde2.Serializer
import org.apache.hadoop.mapred.{TaskID, TaskAttemptID, HadoopWriter}

import shark.{SharkConfVars, SharkEnv, Utils, SharkEnvSlave}
import shark.execution.serialization.OperatorSerializationWrapper
import shark.memstore.TableStats
import shark.memstore2._

import spark.{GrowableAccumulableParam, RDD, TaskContext}
import spark.SparkContext._
import spark.storage.StorageLevel

import tachyon.client.RawColumn
import tachyon.client.RawTable
import tachyon.client.TachyonClient

/**
 * File sink operator. It can accomplish one of the three things:
 * - write query output to disk
 * - cache query output
 * - return query as RDD directly (without materializing it)
 */
class TerminalOperator extends UnaryOperator[HiveFileSinkOperator] {

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

  override def processPartition(split: Int, iter: Iterator[_]): Iterator[_] = iter
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

  override def processPartition(split: Int, iter: Iterator[_]): Iterator[_] = {
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
      val newPart = op.processPartition(-1, iter)
      op.logDebug("Finished executing mapPartitions for operator: " + op)

      true
    }
    writeFiles _
  }
}

/**
 * Cache the RDD and force evaluate it (so the cache is filled).
 */
class CacheSinkOperator extends TerminalOperator {

  @BeanProperty var initialColumnSize: Int = _
  @BeanProperty var storageLevel: StorageLevel = _
  @BeanProperty var tableName: String = _
  @transient var numColumns: Int = _

  override def initializeOnMaster() {
    super.initializeOnMaster()
    initialColumnSize = SharkConfVars.getIntVar(localHconf, SharkConfVars.COLUMN_INITIALSIZE)
  }

  override def initializeOnSlave() {
    super.initializeOnSlave()
    localHconf.setInt(SharkConfVars.COLUMN_INITIALSIZE.varname, initialColumnSize)
  }

  override def execute(): RDD[_] = {
    val inputRdd = if (parentOperators.size == 1) executeParents().head._2 else null

    val statsAcc = SharkEnv.sc.accumulableCollection(ArrayBuffer[(Int, TableStats)]())
    val op = OperatorSerializationWrapper(this)

    var rawTableId: Int = -1;
    if (SharkEnv.useTachyon && (!SharkEnv.selectiveTachyon || tableName.contains("tachyon"))) {
      SharkEnv.tachyonClient.mkdir(SharkEnv.tachyonTableFolder)
      rawTableId = SharkEnv.tachyonClient.createRawTable(
        SharkEnv.tachyonTableFolder + tableName, numColumns + 1)
    }

    // Serialize the RDD on all partitions before putting it into the cache.
    val rdd = inputRdd.mapPartitionsWithIndex { case(split, iter) =>
      op.initializeOnSlave()

      val serde = new ColumnarSerDe
      serde.initialize(op.hconf, op.localHiveOp.getConf.getTableInfo.getProperties())

      var tablePartitionBuilder: Writable = null
      iter.foreach { row =>
        tablePartitionBuilder = serde.serialize(row.asInstanceOf[AnyRef], op.objectInspector)
      }

      if (SharkEnv.useTachyon && (!SharkEnv.selectiveTachyon || tableName.contains("tachyon"))) {
        val rawTable = SharkEnvSlave.tachyonClient.getRawTable(rawTableId)
        val partitionIter =
          if (tablePartitionBuilder != null) {
            var partition = tablePartitionBuilder.asInstanceOf[TablePartitionBuilder].build

            partition.toTachyon.zipWithIndex.foreach { case(buffer, i) =>
              op.logInfo("Filing Column " + i + " partition " + split + " into Tachyon")
              val rawColumn = rawTable.getRawColumn(i)
              rawColumn.createPartition(split)
              val file = rawColumn.getPartition(split)
              file.open("w")
              file.append(buffer)
              file.close()
            }

            partition.iterator
          } else {
            // This partition is empty.
            Iterator()
          }

        //statsAcc += (split, serde.stats)
        partitionIter
      } else {
        val partition =
          if (tablePartitionBuilder != null) {
            Iterator(tablePartitionBuilder.asInstanceOf[TablePartitionBuilder].build)
          } else {
            // This partition is empty.
            Iterator()
          }
        //statsAcc += (split, serde.stats)
        partition
      }
    }

    if (SharkEnv.useTachyon && (!SharkEnv.selectiveTachyon || tableName.contains("tachyon"))) {
    } else{
      // Put the RDD in cache and force evaluate it.
      op.logInfo("Putting RDD for %s in cache, %s %s %s %s".format(
        tableName,
        if (storageLevel.deserialized) "deserialized" else "serialized",
        if (storageLevel.useMemory) "in memory" else "",
        if (storageLevel.useMemory && storageLevel.useDisk) "and" else "",
        if (storageLevel.useDisk) "on disk" else ""))

      SharkEnv.cache.put(tableName, rdd, storageLevel)
    }
    rdd.foreach(_ => Unit)

    // Report remaining memory.
    /* Commented out for now waiting for the reporting code to make into Spark.
    val remainingMems: Map[String, (Long, Long)] = SharkEnv.sc.getSlavesMemoryStatus
    remainingMems.foreach { case(slave, mem) =>
      println("%s: %s / %s".format(
        slave,
        Utils.memoryBytesToString(mem._2),
        Utils.memoryBytesToString(mem._1)))
    }
    println("Summary: %s / %s".format(
      Utils.memoryBytesToString(remainingMems.map(_._2._2).sum),
      Utils.memoryBytesToString(remainingMems.map(_._2._1).sum)))
    */

    // Get the column statistics back to the cache manager.
    SharkEnv.cache.putStats(tableName, statsAcc.value.toMap)

    if (SharkConfVars.getBoolVar(localHconf, SharkConfVars.MAP_PRUNING_PRINT_DEBUG)) {
      statsAcc.value.foreach { case(split, tableStats) =>
        println("Split " + split)
        println(tableStats.toString)
      }
    }

    // Return the cached RDD.
    rdd
  }

  override def processPartition(split: Int, iter: Iterator[_]): Iterator[_] =
    throw new UnsupportedOperationException("CacheSinkOperator.processPartition()")
}


/**
 * Collect the output as a TableRDD.
 */
class TableRddSinkOperator extends TerminalOperator {}
