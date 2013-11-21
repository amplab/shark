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

package shark.util

import org.apache.hadoop.hive.ql.parse.SemanticException 

import shark.memstore2.SharkTblProperties


object QueryRewriteUtils {

  def cacheToAlterTable(cmd: String): String = {
    val cmdSplit = cmd.split(' ')
    if (cmdSplit.size == 2) {
      val tableName = cmdSplit(1)
      """
      ALTER TABLE %s SET TBLPROPERTIES (
        'shark.cache' = 'true',
        'shark.cache.unifyView' = 'true',
        'shark.cache.reloadOnRestart' = 'true')
      """.format(tableName)
    } else {
      throw new SemanticException("CACHE accepts a single table name: 'CACHE <table name>'")
    }
  }

  def uncacheToAlterTable(cmd: String): String = {
    val cmdSplit = cmd.split(' ')
    if (cmdSplit.size == 2) {
      val tableName = cmdSplit(1)
      "ALTER TABLE %s SET TBLPROPERTIES ('shark.cache' = 'false')".format(tableName)
    } else {
      throw new SemanticException("UNCACHE accepts a single table name: 'CACHE <table name>'")
    }  
  }
}
