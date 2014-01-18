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
import java.util.BitSet
import java.util.concurrent.{ConcurrentHashMap => ConcurrentJavaHashMap}

import scala.collection.JavaConversions._

import org.apache.spark.rdd.{EmptyRDD, RDD, UnionRDD}

import tachyon.client.TachyonFS
import tachyon.client.table.{RawTable, RawColumn}

import shark.{LogHelper, SharkEnv}
import shark.execution.serialization.JavaSerializer
import shark.memstore2.{MemoryMetadataManager, TablePartition, TablePartitionStats}


/**
 * An abstraction for the Tachyon APIs.
 */
class TachyonUtilImpl(
    val master: String,
    val warehousePath: String)
  extends TachyonUtil
  with LogHelper {

  private val INSERT_FILE_PREFIX = "insert_"

  private val _fileNameMappings = new ConcurrentJavaHashMap[String, Int]()

  val client = if (master != null && master != "") TachyonFS.get(master) else null

  private def getUniqueFilePath(parentDirectory: String): String = {
    val parentDirectoryLower = parentDirectory.toLowerCase
    val currentInsertNum = if (_fileNameMappings.containsKey(parentDirectoryLower)) {
      _fileNameMappings.get(parentDirectoryLower)
    } else {
      0
    }
    var nextInsertNum = currentInsertNum + 1
    var filePath = parentDirectoryLower + "/" + INSERT_FILE_PREFIX
    // Make sure there aren't file conflicts. This could occur if the directory was created in a
    // previous Shark session.
    while (client.exist(filePath + nextInsertNum)) {
      nextInsertNum = nextInsertNum + 1
    }
    _fileNameMappings.put(parentDirectoryLower, nextInsertNum)
    filePath + nextInsertNum
  }

  if (master != null && warehousePath == null) {
    throw new TachyonException("TACHYON_MASTER is set. However, TACHYON_WAREHOUSE_PATH is not.")
  }

  private def getPath(tableKey: String, hivePartitionKeyOpt: Option[String]): String = {
    val hivePartitionKey = if (hivePartitionKeyOpt.isDefined) {
      "/" + hivePartitionKeyOpt.get
    } else {
      ""
    }
    warehousePath + "/" + tableKey + hivePartitionKey
  }

  override def pushDownColumnPruning(rdd: RDD[_], columnUsed: BitSet): Boolean = {
    val isTachyonTableRdd = rdd.isInstanceOf[TachyonTableRDD]
    if (isTachyonTableRdd) {
      rdd.asInstanceOf[TachyonTableRDD].setColumnUsed(columnUsed)
    }
    isTachyonTableRdd
  }

  override def tachyonEnabled(): Boolean =
    (master != null && warehousePath != null && client.isConnected)

  override def tableExists(tableKey: String, hivePartitionKeyOpt: Option[String]): Boolean = {
    client.exist(getPath(tableKey, hivePartitionKeyOpt))
  }

  override def dropTable(tableKey: String, hivePartitionKeyOpt: Option[String]): Boolean = {
    // The second parameter (true) means recursive deletion.
    client.delete(getPath(tableKey, hivePartitionKeyOpt), true)
  }

  override def createDirectory(
      tableKey: String,
      hivePartitionKeyOpt: Option[String]): Boolean = {
    client.mkdir(getPath(tableKey, hivePartitionKeyOpt))
  }

  override def renameDirectory(
      oldTableKey: String,
      newTableKey: String): Boolean = {
    val oldPath = getPath(oldTableKey, hivePartitionKeyOpt = None)
    val newPath = getPath(newTableKey, hivePartitionKeyOpt = None)
    client.rename(oldPath, newPath)
  }

  override def createRDD(
      tableKey: String,
      hivePartitionKeyOpt: Option[String]
    ): Seq[(RDD[TablePartition], collection.Map[Int, TablePartitionStats])] = {
    // Create a TachyonTableRDD for each raw table file in the directory.
    val tableDirectory = getPath(tableKey, hivePartitionKeyOpt)
    val files = client.ls(tableDirectory, false /* recursive */)
    // The first path is just "{tableDirectory}/", so ignore it.
    val rawTableFiles = files.subList(1, files.size)
    val tableRDDsAndStats = rawTableFiles.map { filePath =>
      val serializedMetadata = client.getRawTable(client.getFileId(filePath)).getMetadata
      val indexToStats = JavaSerializer.deserialize[collection.Map[Int, TablePartitionStats]](
        serializedMetadata.array())
      (new TachyonTableRDD(filePath, SharkEnv.sc), indexToStats)
    }
    tableRDDsAndStats
  }

  override def createTableWriter(
      tableKey: String,
      hivePartitionKeyOpt: Option[String],
      numColumns: Int): TachyonTableWriter = {
    if (!client.exist(warehousePath)) {
      client.mkdir(warehousePath)
    }
    val parentDirectory = getPath(tableKey, hivePartitionKeyOpt)
    val filePath = getUniqueFilePath(parentDirectory)
    new TachyonTableWriterImpl(filePath, numColumns)
  }
}
