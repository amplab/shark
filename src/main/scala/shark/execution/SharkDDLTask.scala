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

package shark.execution

import scala.collection.JavaConversions._

import org.apache.hadoop.hive.ql.{Context, DriverContext}
import org.apache.hadoop.hive.ql.exec.{Task => HiveTask, TaskExecutionException}
import org.apache.hadoop.hive.ql.metadata.Hive
import org.apache.hadoop.hive.ql.plan._
import org.apache.hadoop.hive.ql.plan.api.StageType

import shark.{LogHelper, SharkEnv}
import shark.memstore2.{CacheType, MemoryMetadataManager}


private[shark] class SharkDDLWork(val ddlDesc: DDLDesc) extends java.io.Serializable {
  // Used only for CREATE TABLE.
  var cacheMode: CacheType.CacheType = _
}

private[shark] class SharkDDLTask extends HiveTask[SharkDDLWork] with Serializable with LogHelper {

  override def execute(driverContext: DriverContext): Int = {
    val hiveMetadataDb = Hive.get(conf)

    work.ddlDesc match {
      case alterTableDesc: AlterTableDesc => {
        alterTable(hiveMetadataDb, alterTableDesc)
      }
      case _ => {
        throw new UnsupportedOperationException(
          "Shark does not require a Spark DDL task for: " + work.ddlDesc.getClass.getName)
      }
    }

    // Hive's task runner expects a '0' return value to indicate success and exceptions on
    // failure.
    return 0
  }

  def alterTable(
      hiveMetadataDb: Hive,
      alterTableDesc: AlterTableDesc) {
    if (alterTableDesc.getOp() == AlterTableDesc.AlterTableTypes.RENAME) {
      val oldName = alterTableDesc.getOldName
      val newName = alterTableDesc.getNewName
      SharkEnv.memoryMetadataManager.rename(oldName, newName)
    }
  }

  override def getType = StageType.DDL

  override def getName = "DDL-SPARK"

  override def localizeMRTmpFilesImpl(ctx: Context) = Unit
}
