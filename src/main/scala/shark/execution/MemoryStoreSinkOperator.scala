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

import java.nio.ByteBuffer

import scala.collection.mutable.ArrayBuffer
import scala.reflect.BeanProperty

import org.apache.hadoop.io.Writable

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import shark.{SharkConfVars, SharkEnv}
import shark.execution.serialization.{OperatorSerializationWrapper, JavaSerializer}
import shark.memstore2._
import shark.tachyon.TachyonTableWriter


/**
 * Cache the RDD and force evaluate it (so the cache is filled).
 */
class MemoryStoreSinkOperator extends TerminalOperator {

  @BeanProperty var partitionSize: Int = _
  @BeanProperty var shouldCompress: Boolean = _
  @BeanProperty var storageLevel: StorageLevel = _
  @BeanProperty var tableName: String = _
  @transient var useTachyon: Boolean = _
  @transient var useUnionRDD: Boolean = _
  @transient var numColumns: Int = _

  override def initializeOnMaster() {
    super.initializeOnMaster()
    partitionSize = SharkConfVars.getIntVar(localHconf, SharkConfVars.COLUMN_BUILDER_PARTITION_SIZE)
    shouldCompress = SharkConfVars.getBoolVar(localHconf, SharkConfVars.COLUMNAR_COMPRESSION)
  }

  override def initializeOnSlave() {
    super.initializeOnSlave()
    localHconf.setInt(SharkConfVars.COLUMN_BUILDER_PARTITION_SIZE.varname, partitionSize)
    localHconf.setBoolean(SharkConfVars.COLUMNAR_COMPRESSION.varname, shouldCompress)
  }

  override def execute(): RDD[_] = {
    val inputRdd = if (parentOperators.size == 1) executeParents().head._2 else null

    val statsAcc = SharkEnv.sc.accumulableCollection(ArrayBuffer[(Int, TablePartitionStats)]())
    val op = OperatorSerializationWrapper(this)

    val tachyonWriter: TachyonTableWriter =
      if (useTachyon) {
        // Use an additional row to store metadata (e.g. number of rows in each partition).
        SharkEnv.tachyonUtil.createTableWriter(tableName, numColumns + 1)
      } else {
        null
      }

    // Put all rows of the table into a set of TablePartition's. Each partition contains
    // only one TablePartition object.
    var rdd: RDD[TablePartition] = inputRdd.mapPartitionsWithIndex { case(partitionIndex, iter) =>
      op.initializeOnSlave()
      val serde = new ColumnarSerDe
      serde.initialize(op.localHconf, op.localHiveOp.getConf.getTableInfo.getProperties)

      // Serialize each row into the builder object.
      // ColumnarSerDe will return a TablePartitionBuilder.
      var builder: Writable = null
      iter.foreach { row =>
        builder = serde.serialize(row.asInstanceOf[AnyRef], op.objectInspector)
      }

      if (builder != null) {
        statsAcc += Tuple2(partitionIndex, builder.asInstanceOf[TablePartitionBuilder].stats)
        Iterator(builder.asInstanceOf[TablePartitionBuilder].build)
      } else {
        // Empty partition.
        statsAcc += Tuple2(partitionIndex, new TablePartitionStats(Array(), 0))
        Iterator(new TablePartition(0, Array()))
      }
    }

    if (tachyonWriter != null) {
      // Put the table in Tachyon.
      op.logInfo("Putting RDD for %s in Tachyon".format(tableName))

      SharkEnv.memoryMetadataManager.put(tableName, rdd)

      tachyonWriter.createTable(ByteBuffer.allocate(0))
      rdd = rdd.mapPartitionsWithIndex { case(partitionIndex, iter) =>
        val partition = iter.next()
        partition.toTachyon.zipWithIndex.foreach { case(buf, column) =>
          tachyonWriter.writeColumnPartition(column, partitionIndex, buf)
        }
        Iterator(partition)
      }
      // Force evaluate so the data gets put into Tachyon.
      rdd.context.runJob(rdd, (iter: Iterator[TablePartition]) => iter.foreach(_ => Unit))
    } else {
      // Put the table in Spark block manager.
      op.logInfo("Putting %sRDD for %s in Spark block manager, %s %s %s %s".format(
        if (useUnionRDD) "Union" else "",
        tableName,
        if (storageLevel.deserialized) "deserialized" else "serialized",
        if (storageLevel.useMemory) "in memory" else "",
        if (storageLevel.useMemory && storageLevel.useDisk) "and" else "",
        if (storageLevel.useDisk) "on disk" else ""))

      // Force evaluate so the data gets put into Spark block manager.
      rdd.persist(storageLevel)
      rdd.context.runJob(rdd, (iter: Iterator[TablePartition]) => iter.foreach(_ => Unit))

      val origRdd = rdd
      if (useUnionRDD) {
        // If this is an insert, find the existing RDD and create a union of the two, and then
        // put the union into the meta data tracker.
        rdd = rdd.union(
          SharkEnv.memoryMetadataManager.get(tableName).get.asInstanceOf[RDD[TablePartition]])
      }
      SharkEnv.memoryMetadataManager.put(tableName, rdd)
      rdd.setName(tableName)

      // Run a job on the original RDD to force it to go into cache.
      origRdd.context.runJob(origRdd, (iter: Iterator[TablePartition]) => iter.foreach(_ => Unit))
    }

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

    val columnStats =
      if (useUnionRDD) {
        // Combine stats for the two tables being combined.
        val numPartitions = statsAcc.value.toMap.size
        val currentStats = statsAcc.value
        val otherIndexToStats = SharkEnv.memoryMetadataManager.getStats(tableName).get
        for ((otherIndex, tableStats) <- otherIndexToStats) {
          currentStats.append((otherIndex + numPartitions, tableStats))
        }
        currentStats.toMap
      } else {
        statsAcc.value.toMap
      }

    // Get the column statistics back to the cache manager.
    SharkEnv.memoryMetadataManager.putStats(tableName, columnStats)

    if (tachyonWriter != null) {
      tachyonWriter.updateMetadata(ByteBuffer.wrap(JavaSerializer.serialize(columnStats)))
    }

    if (SharkConfVars.getBoolVar(localHconf, SharkConfVars.MAP_PRUNING_PRINT_DEBUG)) {
      columnStats.foreach { case(index, tableStats) =>
        println("Partition " + index + " " + tableStats.toString)
      }
    }

    // Return the cached RDD.
    rdd
  }

  override def processPartition(split: Int, iter: Iterator[_]): Iterator[_] =
    throw new UnsupportedOperationException("CacheSinkOperator.processPartition()")
}
