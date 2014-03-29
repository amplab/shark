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

import tachyon.client.WriteType

import shark.LogHelper
import shark.execution.serialization.JavaSerializer
import shark.memstore2.{OffHeapStorageClient, OffHeapTableWriter, TablePartitionStats}

class TachyonOffHeapTableWriter(@transient path: String, @transient numColumns: Int)
  extends OffHeapTableWriter with LogHelper {

  // Re-instantiated upon deserialization, the first time it's referenced.
  @transient lazy val tfs = OffHeapStorageClient.client.asInstanceOf[TachyonStorageClient].tfs

  var rawTableId: Int = -1

  override def createTable() {
    val metadata = ByteBuffer.allocate(0)
    rawTableId = tfs.createRawTable(path, numColumns, metadata)
  }

  override def setStats(indexToStats: collection.Map[Int, TablePartitionStats]) {
    val buffer = ByteBuffer.wrap(JavaSerializer.serialize(indexToStats))
    tfs.updateRawTableMetadata(rawTableId, buffer)
  }

  // rawTable is a lazy val so it gets created the first time it is referenced.
  // This is only used on worker nodes.
  @transient lazy val rawTable = tfs.getRawTable(rawTableId)

  override def writeColumnPartition(column: Int, part: Int, data: ByteBuffer) {
    val rawColumn = rawTable.getRawColumn(column)
    rawColumn.createPartition(part)
    val file = rawColumn.getPartition(part)
    val outStream = file.getOutStream(WriteType.CACHE_THROUGH)
    outStream.write(data.array(), 0, data.limit())
    outStream.close()
  }
}
