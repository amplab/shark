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

import scala.collection.JavaConversions._

import java.util.ArrayList

import org.apache.commons.lang.exception.ExceptionUtils
import org.apache.hadoop.hive.metastore.api.{FieldSchema, Schema}
import org.apache.hadoop.hive.ql.Driver
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse
import org.apache.spark.sql.hive.{CatalystContext, HiveMetastoreTypes, HiveResponse}

class CatalystDriver(val context: CatalystContext = CatalystEnv.cc) extends Driver with LogHelper {
  private var tableSchema: Schema = _
  private var hiveResponse: HiveResponse = _

  override def init(): Unit = {
  }

  private def getResultSetSchema(query: context.HiveQLQueryExecution): Schema = {
    val analyzed = query.analyzed
    logger.debug(s"Result Schema: ${analyzed.output}")
    if (analyzed.output.size == 0) {
      new Schema(new FieldSchema("Response code", "string", "") :: Nil, null)
    } else {
      val fieldSchemas = analyzed.output.map { attr =>
        new FieldSchema(attr.name, HiveMetastoreTypes.toMetastoreType(attr.dataType), "")
      }

      new Schema(fieldSchemas, null)
    }
  }

  override def run(command: String): CommandProcessorResponse = {
    val execution = new context.HiveQLQueryExecution(command)

    // TODO unify the error code
    try {
      hiveResponse = execution.result()
      tableSchema = getResultSetSchema(execution)

      hiveResponse match {
        case HiveResponse(responseCode, results, Some(cause)) if responseCode != 0 =>
          logError(s"Failed in [$command]", cause)
          new CommandProcessorResponse(responseCode, ExceptionUtils.getFullStackTrace(cause), null)

        case HiveResponse(responseCode, _, _) =>
          new CommandProcessorResponse(responseCode)
      }
    } catch {
      case cause: Throwable =>
        logError(s"Failed in [$command]", cause)
        new CommandProcessorResponse(-3, ExceptionUtils.getFullStackTrace(cause), null)
    }
  }

  override def close(): Int = {
    hiveResponse = null
    tableSchema = null
    0
  }

  override def getSchema: Schema = tableSchema

  override def getResults(res: ArrayList[String]): Boolean = {
    if(hiveResponse == null) {
      false
    } else {
      res.addAll(hiveResponse.result)
      hiveResponse = null
      true
    }
  }

  override def destroy() {
    super.destroy()
    hiveResponse = null
    tableSchema = null
  }
}
