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

import java.util.{BitSet => JBitSet}
import java.util.concurrent.{ConcurrentHashMap => ConcurrentJavaHashMap}

import scala.collection.JavaConversions._

import org.apache.spark.rdd.{RDD, UnionRDD}
import tachyon.client.TachyonFS

import shark.{LogHelper, SharkEnv}
import shark.execution.TableReader.PruningFunctionType
import shark.execution.serialization.JavaSerializer
import shark.memstore2.{OffHeapStorageClient, OffHeapStorageClientFactory, TablePartitionStats}

class TachyonStorageClientFactory extends OffHeapStorageClientFactory {
  def createClient() = {
    new TachyonStorageClient(
      System.getenv("TACHYON_MASTER"), System.getenv("TACHYON_WAREHOUSE_PATH"))
  }
}

/**
 * Enables caching of Shark tables in native column-oriented format into Tachyon.
 *
 * The directory structure for a given table in Tachyon looks like:
 * Data:     warehouse/database.table/_defaultkey/insert_#/col_#/part_#
 * Metadata: warehouse/database.table/_defaultkey/insert_#/.meta
 * where:
 *   - insert_# is used to allow inserting data multiple times. Files cannot be appended to in
 *     Tachyon, so we instead create a whole new directory and union them upon read.
 *   - col_# is the folder for the particular column
 *   - part_# is the Spark partition for the column
 *
 * Note that "_defaultkey" is the name of the Hive Partition for a non-partitioned table. If the
 * table is partitioned, it will be replaced by each "hivePartitionKey".
 */
class TachyonStorageClient(val master: String, val warehousePath: String)
  extends OffHeapStorageClient with LogHelper {

  /** We create a new directory with a new RawTable for each independent insert. */
  private val INSERT_FILE_PREFIX = "insert_"

  /** Non-partitioned tables use a default partition name for consistency. */
  private val DEFAULT_PARTITION = "_defaultkey"

  private val _fileNameMappings = new ConcurrentJavaHashMap[String, Int]()

  if (master == null || master == "") {
    throw new TachyonException("TACHYON_MASTER is not set, cannot create TachyonStorageClient.")
  }
  if (warehousePath == null) {
    throw new TachyonException("TACHYON_WAREHOUSE is not set, cannot create TachyonStorageClient.")
  }

  val tfs = TachyonFS.get(master)

  private def getUniqueFilePath(parentDirectory: String): String = {
    val parentDirectoryLower = parentDirectory.toLowerCase
    val currentInsertNum = if (_fileNameMappings.containsKey(parentDirectoryLower)) {
      _fileNameMappings.get(parentDirectoryLower)
    } else {
      0
    }
    var nextInsertNum = currentInsertNum + 1
    val filePath = parentDirectoryLower + "/" + INSERT_FILE_PREFIX
    // Make sure there aren't file conflicts. This could occur if the directory was created in a
    // previous Shark session.
    while (tfs.exist(filePath + nextInsertNum)) {
      nextInsertNum = nextInsertNum + 1
    }
    _fileNameMappings.put(parentDirectoryLower, nextInsertNum)
    filePath + nextInsertNum
  }

  private def getTablePath(tableKey: String): String = {
    warehousePath + "/" + tableKey
  }

  private def getPartitionPath(tableKey: String, hivePartitionKey: String): String = {
    getTablePath(tableKey) + "/" + hivePartitionKey
  }

  override def tableExists(tableKey: String): Boolean = {
    tfs.exist(getTablePath(tableKey))
  }

  override def tablePartitionExists(tableKey: String, hivePartitionKey: Option[String]): Boolean = {
    tfs.exist(getPartitionPath(tableKey, hivePartitionKey.getOrElse(DEFAULT_PARTITION)))
  }

  override def dropTable(tableKey: String): Boolean = {
    tfs.delete(getTablePath(tableKey), true /* recursively */)
  }

  override def dropTablePartition(tableKey: String, hivePartitionKey: Option[String]): Boolean = {
    tfs.delete(getPartitionPath(tableKey, hivePartitionKey.getOrElse(DEFAULT_PARTITION)),
      true /* recursively */)
  }

  /**
   * Reads a particular Hive partition (or whole table if non-partitioned) into a single RDD.
   * Since each insert is written to its own directory, we need to reconstruct this RDD by reading
   * across all of these insert directories. We thus column- and row-prune our data before
   * producing the union to avoid data transfer.
   *
   * @param columnsUsed Indicates which columns are needed, to avoid loading extra data.
   * @param pruningFn Used for pruning rows.
   */
  override def readTablePartition(
      tableKey: String,
      hivePartitionKey: Option[String],
      columnsUsed: JBitSet,
      pruningFn: PruningFunctionType
    ): RDD[_] = {

    try {
      if (!tablePartitionExists(tableKey, hivePartitionKey)) {
        throw new TachyonException("Table " + tableKey + " does not exist in Tachyon")
      }

      // Create a TachyonTableRDD for each raw tableRDDsAndStats file in the directory.
      val tableDirectory = getPartitionPath(tableKey, hivePartitionKey.getOrElse(DEFAULT_PARTITION))
      val files = tfs.ls(tableDirectory, false /* recursive */)
      // The first path is just "{tableDirectory}/", so ignore it.
      val rawTableFiles = files.subList(1, files.size)
      val prunedRDDs = rawTableFiles.map { filePath =>
        val serializedMetadata = tfs.getRawTable(tfs.getFileId(filePath)).getMetadata
        val indexToStats = JavaSerializer.deserialize[collection.Map[Int, TablePartitionStats]](
          serializedMetadata.array())
       pruningFn(new TachyonTableRDD(filePath, columnsUsed, SharkEnv.sc), indexToStats)
      }
      new UnionRDD(SharkEnv.sc, prunedRDDs.toSeq.asInstanceOf[Seq[RDD[Any]]])
    } catch {
      case e: Exception =>
        logError("Exception while reading table partition", e)
        throw e
    }
  }

  override def createTablePartitionWriter(
      tableKey: String,
      hivePartitionKey: Option[String],
      numColumns: Int): TachyonOffHeapTableWriter = {
    if (!tfs.exist(warehousePath)) {
      tfs.mkdir(warehousePath)
    }
    val parentDirectory = getPartitionPath(tableKey, hivePartitionKey.getOrElse(DEFAULT_PARTITION))
    val filePath = getUniqueFilePath(parentDirectory)
    new TachyonOffHeapTableWriter(filePath, numColumns)
  }

  override def createTablePartition(
      tableKey: String,
      hivePartitionKeyOpt: Option[String]): Boolean = {
    hivePartitionKeyOpt match {
      case Some(key) => tfs.mkdir(getPartitionPath(tableKey, key))
      case None => tfs.mkdir(getPartitionPath(tableKey, DEFAULT_PARTITION))
    }
  }

  override def renameTable(
      oldTableKey: String,
      newTableKey: String): Boolean = {
    val oldPath = getTablePath(oldTableKey)
    val newPath = getTablePath(newTableKey)
    tfs.rename(oldPath, newPath)
  }
}
