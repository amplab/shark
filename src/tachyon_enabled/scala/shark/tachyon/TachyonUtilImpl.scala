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

import scala.collection.JavaConverters._

import org.apache.spark.rdd.RDD

import tachyon.client.TachyonFS
import tachyon.client.table.{RawTable, RawColumn}

import shark.SharkEnv
import shark.memstore2.TablePartition


/**
 * An abstraction for the Tachyon APIs.
 */
class TachyonUtilImpl(val master: String, val warehousePath: String) extends TachyonUtil {

  val client = if (master != null && master != "") TachyonFS.get(master) else null

  if (master != null && warehousePath == null) {
    throw new TachyonException("TACHYON_MASTER is set. However, TACHYON_WAREHOUSE_PATH is not.")
  }

  def getPath(tableName: String): String = warehousePath + "/" + tableName

  override def pushDownColumnPruning(rdd: RDD[_], columnUsed: BitSet): Boolean = {
    if (rdd.isInstanceOf[TachyonTableRDD]) {
      rdd.asInstanceOf[TachyonTableRDD].setColumnUsed(columnUsed)
      true
    } else {
      false
    }
  }


  override def tachyonEnabled(): Boolean = (master != null && warehousePath != null)

  override def tableExists(tableName: String): Boolean = {
    client.exist(getPath(tableName))
  }

  override def dropTable(tableName: String): Boolean = {
    // The second parameter (true) means recursive deletion.
    client.delete(getPath(tableName), true)
  }

  override def getTableMetadata(tableName: String): ByteBuffer = {
    if (!tableExists(tableName)) {
       throw new TachyonException("Table " + tableName + " does not exist in Tachyon")
    }
    client.getRawTable(getPath(tableName)).getMetadata()
  }

  override def createRDD(tableName: String): RDD[TablePartition] = {
    new TachyonTableRDD(getPath(tableName), SharkEnv.sc)
  }

  override def createTableWriter(tableName: String, numColumns: Int): TachyonTableWriter = {
    if (!client.exist(warehousePath)) {
      client.mkdir(warehousePath)
    }
    new TachyonTableWriterImpl(getPath(tableName), numColumns)
  }
}
