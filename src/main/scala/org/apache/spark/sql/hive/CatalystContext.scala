/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hive

import scala.collection.JavaConversions._

import java.util.{ArrayList => JArrayList}

import org.apache.hadoop.hive.ql.Driver
import org.apache.hadoop.hive.ql.processors._
import org.apache.hadoop.hive.ql.session.SessionState
import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, NativeCommand}
import shark.LogHelper

case class HiveResponse(responseCode: Int, result: Seq[String], exception: Option[Throwable])

//TODO work around for HiveContext, need to update that in Spark project (sql/hive), not here.
case class CatalystContext(sc: SparkContext) extends HiveContext(sc) with LogHelper {
  @transient protected[hive] override lazy val hiveconf = sessionState.getConf()
  @transient protected[hive] override lazy val sessionState = SessionState.get()

  class HiveQLQueryExecution(hql: String) extends QueryExecution {
    override def logical: LogicalPlan = HiveQl.parseSql(hql)
    override def toString = hql + "\n" + super.toString

    /**
     * Query Result (responseCode, result, exception if any)
     * If response code equals 0 means got the result, otherwise failed due to some reason/exception
     */
    def result(): HiveResponse = analyzed match {
      case NativeCommand(cmd) => runOnHive(cmd)
      case query =>
        try {
          // We need the types so we can output struct field names
          val types = analyzed.output.map(_.dataType)
          // Reformat to match hive tab delimited output.
          val result = toRdd.collect().map(_.zip(types).map(toHiveString).mkString("\t"))
          HiveResponse(0, result, None)
        } catch {
          case cause: Throwable => {
            logError("Error:\n $cmd\n", cause)
            HiveResponse(-1, Seq.empty[String], Some(cause))
          }
        }
    }
  }

  // TODO (lian) We should make HiveContext.runHive behave similarly to remove CatalystContext
  // See: https://issues.apache.org/jira/browse/SPARK-2106
  def runOnHive(cmd: String, maxRows: Int = 1000): HiveResponse = {
    try {
      val cmd_trimmed: String = cmd.trim()
      val tokens: Array[String] = cmd_trimmed.split("\\s+")
      val cmd_1: String = cmd_trimmed.substring(tokens(0).length()).trim()
      val proc: CommandProcessor = CommandProcessorFactory.get(tokens(0), hiveconf)

      proc match {
        case driver: Driver =>
          driver.init()

          val results = new JArrayList[String]
          val response: CommandProcessorResponse = driver.run(cmd)
          // Throw an exception if there is an error in query processing.
          if (response.getResponseCode != 0) {
            driver.destroy()
            HiveResponse(
              response.getResponseCode,
              Seq[String](response.getErrorMessage()),
              Some(new Exception(cmd)))
          } else {
            driver.setMaxRows(maxRows)
            driver.getResults(results)
            driver.destroy()
            HiveResponse(0, results, None)
          }
        case _ =>
          SessionState.get().out.println(tokens(0) + " " + cmd_1)
          val res = proc.run(cmd_1)
          if(res.getResponseCode == 0) {
            HiveResponse(0, Seq.empty[String], None)
          } else {
            HiveResponse(res.getResponseCode, Seq(res.getErrorMessage), Some(new Exception(cmd_1)))
          }
      }
    } catch {
      case e: Throwable =>
        logger.error(
          s"""
            |======================
            |HIVE FAILURE OUTPUT
            |======================
            |${outputBuffer.toString}
            |======================
            |END HIVE FAILURE OUTPUT
            |======================
          """.stripMargin)
        HiveResponse(-2, Seq[String](), Some(e))
    }
  }
}
