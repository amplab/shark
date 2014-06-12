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

package shark

import java.util.ArrayList

import scala.collection.JavaConversions._

import org.apache.commons.lang.exception.ExceptionUtils

import org.apache.hive.service.cli.TableSchema

import org.apache.hadoop.hive.ql.Driver
import org.apache.hadoop.hive.metastore.api.Schema
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse

import org.apache.spark.sql.hive.CatalystContext

class CatalystDriver(val context: CatalystContext = CatalystEnv.cc) extends Driver with LogHelper {
  private var tschema: TableSchema = _
  private var result: (Int, Seq[String], Throwable) = _
  
  override def init(): Unit = {
  }
  
  override def run(command: String): CommandProcessorResponse = {
    val execution = new context.HiveQLQueryExecution(command)

    // TODO unify the error code
    try {
      result = execution.result
      tschema = execution.getResultSetSchema
      
      if(result._1 != 0) {
        logError(s"Failed in [$command]", result._3)
        new CommandProcessorResponse(result._1, ExceptionUtils.getFullStackTrace(result._3), null)
      } else {
        new CommandProcessorResponse(result._1)
      }
    } catch {
      case t: Throwable => 
        logError(s"Failed in [$command]", t)
        new CommandProcessorResponse(-3, ExceptionUtils.getFullStackTrace(t), null)
    }
  }
  
  override def close(): Int = {
    result = null
    tschema = null
    
    0
  }

  /**
   * Get the result schema, currently CatalystDriver doesn't support it yet.
   * TODO: the TableSchema (org.apache.hive.service.cli.TableSchema) is returned by Catalyst, 
   * however, the Driver requires the Schema (org.apache.hadoop.hive.metastore.api.Schema)
   * Need to figure out how to convert the previous to later.
   */
  override def getSchema(): Schema = throw new UnsupportedOperationException("for getSchema")
  def getTableSchema = tschema
  
  override def getResults(res: ArrayList[String]): Boolean = {
    if(result == null) {
      false
    } else {
      res.addAll(result._2)
      result = null
      true
    }
  }
  
  override def destroy() {
    result = null
    tschema = null
  }
}