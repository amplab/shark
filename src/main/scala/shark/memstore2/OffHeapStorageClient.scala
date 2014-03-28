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

package shark.memstore2

import java.util
import java.nio.ByteBuffer
import java.util.BitSet

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import shark.LogHelper
import shark.execution.TableReader.PruningFunctionType

/**
 * Provides an API for writing to an off-heap storage service, such as Tachyon.
 *
 * To ease the distinction between partitioned and non-partitioned tables, we consider a
 * non-partitioned table as a special case of partitioned tables with the single hivePartitionKey
 * with value None.
 */
abstract class OffHeapStorageClient {

  def tableExists(tableKey: String): Boolean

  def tablePartitionExists(tableKey: String, hivePartitionKey: Option[String]): Boolean

  def dropTable(tableKey: String): Boolean

  def dropTablePartition(tableKey: String, hivePartitionKey: Option[String]): Boolean

  def createTablePartitionWriter(
      tableKey: String,
      hivePartitionKeyOpt: Option[String],
      numColumns: Int)
    : OffHeapTableWriter

  /**
   * Reads a partition into an RDD, projecting columns and pruning rows to reduce total amount of
   * data transferred out of the off-heap storage.
   * In order to facilitate the column projection, the RDD may return a TablePartition with null
   * column ByteBuffers, which the row-pruning function will safely ignore.
   */
  def readTablePartition(
      tableKey: String,
      hivePartitionKeyOpt: Option[String],
      columnsUsed: util.BitSet,
      pruningFn: PruningFunctionType)
    : RDD[_]

  def createTablePartition(tableKey: String, hivePartitionKeyOpt: Option[String]): Boolean

  def renameTable(oldTableKey: String, newTableKey: String): Boolean
}

abstract class OffHeapTableWriter extends Serializable {

  /** Create a table in Tachyon. Called only on the driver node. */
  def createTable()

  /** Update the metadata in Tachyon. Called only on the driver node. */
  def setStats(indexToStats: collection.Map[Int, TablePartitionStats])

  /** Write the data of a partition of a given column to Tachyon. Called only on worker nodes. */
  def writeColumnPartition(column: Int, part: Int, data: ByteBuffer)
}

trait OffHeapStorageClientFactory {
  def createClient(): OffHeapStorageClient
}

object OffHeapStorageClient extends LogHelper {

  lazy val clientFactoryClassName: String = sys.props.get("shark.offheap.clientFactory")
    .orElse(sys.env.get("SHARK_OFFHEAP_CLIENT_FACTORY"))
    .getOrElse("shark.tachyon.TachyonStorageClientFactory")

  lazy val client: OffHeapStorageClient = {
    val clientFactoryClass = Class.forName(clientFactoryClassName)
    clientFactoryClass.getConstructors

    // First try with the constructor that takes SparkConf. If we can't find one,
    // use a no-arg constructor instead.
    try {
      val constructor = clientFactoryClass.getConstructor()
      val clientFactory = constructor.newInstance().asInstanceOf[OffHeapStorageClientFactory]
      logInfo("Creating client with factory: " + clientFactoryClassName)
      clientFactory.createClient()
    } catch {
      case _: NoSuchMethodException => {
        logInfo("No off-heap storage client loaded.")
        null
      }
    }
  }
}
