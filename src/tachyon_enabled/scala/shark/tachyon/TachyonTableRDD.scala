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

import java.io.EOFException
import java.nio.ByteBuffer
import java.util.NoSuchElementException

import scala.collection.JavaConverters._

import shark.{SharkEnv, SharkEnvSlave}
import shark.memstore2._

import spark.{Dependency, Partition, RDD, SerializableWritable, SparkContext, TaskContext}

import tachyon.client.InStream
import tachyon.client.OpType
import tachyon.client.RawTable
import tachyon.client.TachyonFile

private class TachyonTablePartition(rddId: Int, idx: Int, val locations: Seq[String])
  extends Partition {

  override def hashCode(): Int = (41 * (41 + rddId) + idx).toInt

  override val index: Int = idx
}

/**
 * An RDD that reads a Tachyon Table.
 */
class TachyonTableRDD(path: String, @transient sc: SparkContext)
  extends RDD[ColumnarStruct](sc, Nil) {

  override def getPartitions: Array[Partition] = {
    val rawTable: RawTable = SharkEnv.tachyonUtil.client.getRawTable(path)
    // Use the first column to get preferred locations for all partitions.
    val rawColumn = rawTable.getRawColumn(0)
    val numPartitions: Int = rawColumn.partitions()
    Array.tabulate[Partition](numPartitions) { part =>
      val locations = rawColumn.getPartition(part).getLocationHosts().asScala
      new TachyonTablePartition(id, part, locations) : Partition
    }
  }

  override def compute(theSplit: Partition, context: TaskContext): Iterator[ColumnarStruct] = {
    // TODO: Prune columns - there is no need to read all columns out.
    val rawTable: RawTable = SharkEnvSlave.tachyonUtil.client.getRawTable(path)
    val buffers = Array.tabulate[ByteBuffer](rawTable.getColumns()) { columnIndex =>
      val fp = rawTable.getRawColumn(columnIndex).getPartition(theSplit.index, true)
      var buf: ByteBuffer = fp.readByteBuffer()
      if (buf == null && fp.recacheData()) {
        buf = fp.readByteBuffer()
      }
      if (buf == null) {
        // TODO Log this. Reading data from remote is bad.
        buf = ByteBuffer.allocate(fp.length().toInt)
        val is = fp.getInStream(OpType.READ_TRY_CACHE)
        is.read(buf.array)
        is.close()
        buf.limit(fp.length().toInt)
      }
      buf
    }
    (new TablePartition(buffers)).iterator
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    split.asInstanceOf[TachyonTablePartition].locations
  }

  // override def checkpoint() {
  //   // Do nothing. Tachyon RDD should not be checkpointed.
  // }
}
