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

import scala.reflect.BeanProperty

import shark.{LogHelper, SharkConfVars}
import shark.execution.serialization.JavaSerializer
import shark.memstore2.{OffHeapStorageClient, OffHeapTableWriter, TablePartitionStats}

import tachyon.client.WriteType
import tachyon.master.MasterInfo
import tachyon.util.CommonUtils

class TachyonOffHeapTableWriter(@transient path: String, @transient numColumns: Int)
  extends OffHeapTableWriter with LogHelper {

  // Re-instantiated upon deserialization, the first time it's referenced.
  @transient lazy val tfs = OffHeapStorageClient.client.asInstanceOf[TachyonStorageClient].tfs
  val TEMP = "_temperary"
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

  override def writePartitionColumn(part: Int, column: Int, data: ByteBuffer, tempDir: String) {
    val tmpPath = CommonUtils.concat(rawTable.getPath(), TEMP)
    val fid = tfs.createFile(CommonUtils.concat(tmpPath, tempDir, column + "", part + ""))
    val file = tfs.getFile(fid)
    val writeType: WriteType = WriteType.valueOf(
        SharkConfVars.getVar(localHconf, SharkConfVars.TACHYON_WRITER_WRITETYPE))
    val outStream = file.getOutStream(writeType)
    outStream.write(data.array(), 0, data.limit())
    outStream.close()
  }

  override def commitPartition(part: Int, numColumns: Int, tempDir: String) {
    val tmpPath = CommonUtils.concat(rawTable.getPath(), TEMP)
    (0 until numColumns).reverse.foreach { column =>
      val srcPath = CommonUtils.concat(tmpPath, tempDir, column + "", part + "")
      val destPath = CommonUtils.concat(rawTable.getPath(), MasterInfo.COL, column + "", part + "")
      tfs.rename(srcPath, destPath)
    }
    tfs.delete(CommonUtils.concat(tmpPath, tempDir), true)
  }

  override def cleanTmpPath() {
    val tmpPath = CommonUtils.concat(rawTable.getPath(), TEMP)
    tfs.delete(tmpPath, true)
  }
}
