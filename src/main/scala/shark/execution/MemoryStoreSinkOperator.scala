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

import scala.collection.mutable.ArrayBuffer
import scala.reflect.BeanProperty

import org.apache.hadoop.io.Writable

import shark.SharkConfVars
import shark.SharkEnv
import shark.SharkEnvSlave
import shark.Utils
import shark.memstore2._
import shark.execution.serialization.OperatorSerializationWrapper

import spark.RDD
import spark.TaskContext
import spark.SparkContext._
import spark.storage.StorageLevel

import tachyon.client.RawColumn
import tachyon.client.RawTable
import tachyon.client.TachyonClient


/**
 * Cache the RDD and force evaluate it (so the cache is filled).
 */
class MemoryStoreSinkOperator extends TerminalOperator {

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

    val statsAcc = SharkEnv.sc.accumulableCollection(ArrayBuffer[(Int, TablePartitionStats)]())
    val op = OperatorSerializationWrapper(this)

    var rawTableId: Int = -1
    // TODO: properly handle where the table goes.
    if (SharkEnv.useTachyon && (!SharkEnv.selectiveTachyon || op.tableName.contains("tachyon"))) {
      SharkEnv.tachyonClient.mkdir(SharkEnv.tachyonTableFolder)
      rawTableId = SharkEnv.tachyonClient.createRawTable(
        SharkEnv.tachyonTableFolder + tableName, numColumns + 1)
    }

    // Serialize the RDD on all partitions before putting it into the cache.
    val rdd = inputRdd.mapPartitionsWithIndex { case(split, iter) =>
      op.initializeOnSlave()

      val serde = new ColumnarSerDe
      serde.initialize(op.hconf, op.localHiveOp.getConf.getTableInfo.getProperties())

      // ColumnarSerDe will return a TablePartitionBuilder.
      var builder: Writable = null
      iter.foreach { row =>
        builder = serde.serialize(row.asInstanceOf[AnyRef], op.objectInspector)
      }

      if (SharkEnv.useTachyon && (!SharkEnv.selectiveTachyon || op.tableName.contains("tachyon"))) {
        val rawTable = SharkEnvSlave.tachyonClient.getRawTable(rawTableId)
        val partitionIter =
          if (builder != null) {
            var partition = builder.asInstanceOf[TablePartitionBuilder].build

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
          if (builder != null) {
            Iterator(builder.asInstanceOf[TablePartitionBuilder].build)
          } else {
            // This partition is empty.
            Iterator()
          }

        statsAcc += Tuple2(split, builder.asInstanceOf[TablePartitionBuilder].stats)
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

      SharkEnv.memoryMetadataManager.put(tableName, rdd, storageLevel)
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
    SharkEnv.memoryMetadataManager.putStats(tableName, statsAcc.value.toMap)

    if (SharkConfVars.getBoolVar(localHconf, SharkConfVars.MAP_PRUNING_PRINT_DEBUG)) {
      statsAcc.value.foreach { case(split, tableStats) =>
        println("Partition " + split + " " + tableStats.toString)
      }
    }

    // Return the cached RDD.
    rdd
  }

  override def processPartition(split: Int, iter: Iterator[_]): Iterator[_] =
    throw new UnsupportedOperationException("CacheSinkOperator.processPartition()")
}
