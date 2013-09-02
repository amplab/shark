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

import java.nio.ByteBuffer

import tachyon.client.OutStream
import tachyon.client.WriteType

import shark.{SharkEnv, SharkEnvSlave}


class TachyonTableWriterImpl(@transient path: String, @transient numColumns: Int)
  extends TachyonTableWriter {

  // rawTableId is used on both driver node and worker nodes.
  var rawTableId: Int = -1

  /** Create a table in Tachyon. Called only on the driver node. */
  override def createTable(metadata: ByteBuffer) {
    rawTableId = SharkEnv.tachyonUtil.client.createRawTable(path, numColumns, metadata)
  }

  /** Update the metadata in Tachyon. Called only on the driver node. */
  override def updateMetadata(metadata: ByteBuffer) {
    SharkEnv.tachyonUtil.client.updateRawTableMetadata(rawTableId, metadata)
  }

  // rawTable is a lazy val so it gets created the first time it is referenced.
  // This is only used on worker nodes.
  @transient lazy val rawTable = SharkEnvSlave.tachyonUtil.client.getRawTable(rawTableId)

  /** Write the data of a partition of a given column to Tachyon. Called only on worker nodes. */
  override def writeColumnPartition(column: Int, part: Int, data: ByteBuffer) {
    val rawColumn = rawTable.getRawColumn(column)
    rawColumn.createPartition(part)
    val file = rawColumn.getPartition(part)
    val outStream = file.getOutStream(WriteType.CACHE_THROUGH)
    outStream.write(data.array(), 0, data.limit())
    outStream.close()
  }
}
