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

package shark.tachyon

import java.nio.{ByteBuffer, ByteOrder}
import java.util.{BitSet => JBitSet}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.{Partition, SparkContext, TaskContext}
import shark.memstore2._
import tachyon.client.{ReadType, TachyonByteBuffer}
import tachyon.client.table.RawTable
import org.apache.spark.rdd.RDD
import shark.LogHelper

private class TachyonTablePartition(rddId: Int, idx: Int, val locations: Seq[String])
  extends Partition {

  override def hashCode(): Int = (41 * (41 + rddId) + idx).toInt

  override val index: Int = idx
}

/**
 * An RDD that reads a Tachyon Table.
 */
class TachyonTableRDD(path: String, columnsUsed: JBitSet, @transient sc: SparkContext)
  extends RDD[TablePartition](sc, Nil) with LogHelper {

  @transient lazy val tfs = OffHeapStorageClient.client.asInstanceOf[TachyonStorageClient].tfs

  override def getPartitions: Array[Partition] = {
    val rawTable: RawTable = tfs.getRawTable(path)
    // Use the first column to get preferred locations for all partitions.
    val rawColumn = rawTable.getRawColumn(0)
    val numPartitions: Int = rawColumn.partitions()
    Array.tabulate[Partition](numPartitions) { part =>
      val locations = rawColumn.getPartition(part).getLocationHosts().asScala
      new TachyonTablePartition(id, part, locations) : Partition
    }
  }

  override def compute(theSplit: Partition, context: TaskContext): Iterator[TablePartition] = {
    val rawTable: RawTable = tfs.getRawTable(path)
    val activeBuffers = new ArrayBuffer[TachyonByteBuffer]()
    val buffers = Array.tabulate[ByteBuffer](rawTable.getColumns()) { columnIndex =>
      if (columnIndex != 0 && columnsUsed != null && !columnsUsed.get(columnIndex - 1)) {
        null
      } else {
        val fp = rawTable.getRawColumn(columnIndex).getPartition(theSplit.index, true)
        // Try to read data from Tachyon's memory, either local or remote.
        var buf = fp.readByteBuffer()
        if (buf == null && fp.recache()) {
          // The data is not in Tachyon's memory yet, recache succeed.
          buf = fp.readByteBuffer()
        }
        if (buf == null) {
          logWarning("Table " + path + " column " + columnIndex + " partition " + theSplit.index
            + " is not in Tachyon's memory. Streaming it in.")
          var data = ByteBuffer.allocate(fp.length().toInt)
          val is = fp.getInStream(ReadType.CACHE)
          is.read(data.array)
          is.close()
          data.limit(fp.length().toInt)
          data
        } else {
          activeBuffers += buf
          buf.DATA
        }
      }
    }

    // Register an on-task-completion callback to close the input stream.
    context.addOnCompleteCallback(() => activeBuffers.foreach(_.close()))

    Iterator(new TablePartition(buffers.map(buffer =>
      if (buffer == null) null else buffer.order(ByteOrder.nativeOrder()))))
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    split.asInstanceOf[TachyonTablePartition].locations
  }

  // override def checkpoint() {
  //   // Do nothing. Tachyon RDD should not be checkpointed.
  // }
}
