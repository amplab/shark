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

import scala.collection.JavaConversions.asScalaBuffer

import org.apache.hadoop.hive.ql.metadata.Hive

import shark.LogHelper
import shark.util.QueryRewriteUtils

/**
 * Singleton used to reload RDDs upon server restarts.
 */
object TableRecovery extends LogHelper {

  val db = Hive.get()

  /**
   * Loads any tables with the `SharkTblProperties.RELOAD_ON_RESTART_FLAG` property set to true.
   * @param cmdRunner The runner that is responsible for taking a cached table query and
   *        a) Creating the table metadata in Hive Meta Store
   *        b) Loading the table as an RDD in memory
   *        @see SharkServer for an example usage.
   */
  def loadUnifiedViews(cmdRunner: String => Unit) {
    // Filter for tables that should be reloaded into the cache.
    val currentDbName = db.getCurrentDatabase()
    for (databaseName <- db.getAllDatabases()) {
      for (tableName <- db.getAllTables(databaseName)) {
        val tblProps = db.getTable(databaseName, tableName).getParameters
        val shouldReload = Option(tblProps.get(SharkTblProperties.RELOAD_ON_RESTART_FLAG.varname)).
          exists(_.toBoolean)
        if (shouldReload) {
          logInfo("Reloading %s.%s into memory.".format(databaseName, tableName))
          // TODO(harvey): Executing SQL directly is a bit of a cop-out...
          val cmd = QueryRewriteUtils.cacheToAlterTable("CACHE %s".format(tableName))
          cmdRunner(cmd)
        }
      }
    }
    db.setCurrentDatabase(currentDbName)
  }
}
