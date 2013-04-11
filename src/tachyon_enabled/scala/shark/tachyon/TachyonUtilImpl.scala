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

import scala.collection.JavaConverters._

import shark.SharkEnv
import shark.memstore2.ColumnarStruct

import spark.RDD

import tachyon.client.{RawTable, RawColumn, TachyonClient}


/**
 * An abstraction for the Tachyon APIs.
 */
class TachyonUtilImpl(val master: String, val warehousePath: String) extends TachyonUtil {

  val client = if (master != null) TachyonClient.getClient(master) else null

  def getPath(tableName: String): String = warehousePath + "/" + tableName

  override def tableExists(tableName: String): Boolean = {
    client.exist(getPath(tableName))
  }

  override def createRDD(tableName: String): RDD[ColumnarStruct] = {
    new TachyonTableRDD(getPath(tableName), SharkEnv.sc)
  }

  override def createTableWriter(tableName: String, numColumns: Int): TachyonTableWriter = {
    client.mkdir(warehousePath)
    new TachyonTableWriterImpl(getPath(tableName), numColumns)
  }
}
