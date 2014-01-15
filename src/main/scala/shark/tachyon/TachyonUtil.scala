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

import org.apache.spark.rdd.RDD

import shark.memstore2.{TablePartition, TablePartitionStats}


/**
 * An abstraction for Tachyon APIs. Specific implementations are provided
 * in the tachyon_enabled and tachyon_disabled folder so we can compile Shark
 * even without Tachyon jars.
 */
abstract class TachyonUtil {

  def pushDownColumnPruning(rdd: RDD[_], columnUsed: BitSet): Boolean

  def tachyonEnabled(): Boolean

  def tableExists(tableKey: String, hivePartitionKeyOpt: Option[String]): Boolean

  def dropTable(tableKey: String, hivePartitionKeyOpt: Option[String]): Boolean

  def createDirectory(tableKey: String, hivePartitionKeyOpt: Option[String]): Boolean

  def renameDirectory(oldName: String, newName: String): Boolean

  def createRDD(
      tableKey: String,
      hivePartitionKeyOpt: Option[String]
    ): Seq[(RDD[TablePartition], collection.Map[Int, TablePartitionStats])]

  def createTableWriter(
  	  tableKey: String,
  	  hivePartitionKey: Option[String],
  	  numColumns: Int
    ): TachyonTableWriter
}
