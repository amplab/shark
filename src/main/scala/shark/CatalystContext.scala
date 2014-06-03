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

package org.apache.spark.sql
package hive

import java.util.{ArrayList => JArrayList}
import scala.collection.JavaConversions._

import org.apache.hive.service.cli.TableSchema
import org.apache.hadoop.hive.metastore.api.FieldSchema
import org.apache.hadoop.hive.cli.CliSessionState
import org.apache.hadoop.hive.cli.CliDriver
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.session.SessionState
import org.apache.hadoop.hive.ql.processors.CommandProcessor
import org.apache.hadoop.hive.ql.processors.CommandProcessorFactory
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse
import org.apache.hadoop.hive.ql.Driver

import org.apache.spark.SparkContext
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.catalyst.plans.logical.NativeCommand
import org.apache.spark.sql.catalyst.plans.logical.ExplainCommand
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.QueryExecutionException

import shark.LogHelper

case class CatalystContext(sc: SparkContext) extends HiveContext(sc) with LogHelper {
  private var result: SchemaRDD = _

  class HiveQLQueryExecution(hql: String) extends QueryExecution {
    override def logical: LogicalPlan = HiveQl.parseSql(hql)
    override def toString = hql + "\n" + super.toString
    
    def result(): (Int, Seq[String]) = analyzed match {
      case NativeCommand(cmd) => runOnHive(cmd)
      case ExplainCommand(plan) => (0, new QueryExecution { val logical = plan }.toString.split("\n"))
      case query =>
        try{
          val result: Seq[Seq[Any]] = toRdd.collect().toSeq
          // We need the types so we can output struct field names
          val types = analyzed.output.map(_.dataType)
          // Reformat to match hive tab delimited output.
          (0, result.map(_.zip(types).map(toHiveString)).map(_.mkString("\t")).toSeq)
        } catch {
          case e: Throwable => {
            logError("Error:\n $cmd\n", e)
            (-1, Seq[String]())
          }
        }
    }
  }
  
  def getResultSetSchema: TableSchema = {
    logger.warn(s"Result Schema: ${result.queryExecution.analyzed.output}")
    if (result.queryExecution.analyzed.output.size == 0) {
      new TableSchema(new FieldSchema("Result", "string", "") :: Nil)
    } else {
      val schema = result.queryExecution.analyzed.output.map { attr =>
        new FieldSchema(attr.name, org.apache.spark.sql.hive.HiveMetastoreTypes.toMetastoreType(attr.dataType), "")
      }
      new TableSchema(schema)
    }
  }

  def runOnHive(cmd: String, maxRows: Int = 1000): (Int, Seq[String]) = {
    try {
      val cmd_trimmed: String = cmd.trim()
      val tokens: Array[String] = cmd_trimmed.split("\\s+")
      val cmd_1: String = cmd_trimmed.substring(tokens(0).length()).trim()
      val proc: CommandProcessor = CommandProcessorFactory.get(tokens(0), hiveconf)

      SessionState.start(sessionState)

      proc match {
        case driver: Driver =>
          driver.init()

          val results = new JArrayList[String]
          val response: CommandProcessorResponse = driver.run(cmd)
          // Throw an exception if there is an error in query processing.
          if (response.getResponseCode != 0) {
            driver.destroy()
            throw new QueryExecutionException(response.getErrorMessage)
          }
          driver.setMaxRows(maxRows)
          driver.getResults(results)
          driver.destroy()
          (0, results)
        case _ =>
          sessionState.out.println(tokens(0) + " " + cmd_1)
          (proc.run(cmd_1).getResponseCode, Seq[String]())
      }
    } catch {
      case e: Exception =>
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
        throw e
    }
  }

  override lazy val hiveconf = new HiveConf(classOf[SessionState])
  override lazy val sessionState = new SessionState(hiveconf)
}

abstract class Launcher(cc: CatalystContext) {
  def execute(hql: String): (Int, Seq[String]) = new cc.HiveQLQueryExecution(hql).result()
}

private[hive] case class HiveLauncher(cc: CatalystContext) extends Launcher(cc) {
  override def execute(hql: String): (Int, Seq[String]) = cc.runOnHive(hql)
}

private[hive] case class SparkLauncher(cc: CatalystContext) extends Launcher(cc)

object CatalystContextWrapper {
  val EXEC_MODE = "catalyst.exec.mode"
  val EXEC_MODE_SPARK = "spark"
  val EXEC_MODE_HIVE  = "hive"
}

class CatalystContextWrapper(cc: CatalystContext) {
  val candidates = (CatalystContextWrapper.EXEC_MODE_SPARK, SparkLauncher(cc)) :: 
                   (CatalystContextWrapper.EXEC_MODE_HIVE, HiveLauncher(cc)) :: Nil
  
//  // Use reflection to get access to the two fields.
//  val getFormattedDbMethod = classOf[CliDriver].getDeclaredMethod(
//    "getFormattedDb", classOf[HiveConf], classOf[CliSessionState])
//  getFormattedDbMethod.setAccessible(true)
//
//  val spacesForStringMethod = classOf[CliDriver].getDeclaredMethod(
//    "spacesForString", classOf[String])
//  spacesForStringMethod.setAccessible(true)

  def env: (String, org.apache.spark.sql.hive.Launcher) = {
    val conf: HiveConf = cc.sessionState.getConf()
//    val db = getFormattedDbMethod.invoke(null, conf, ss).asInstanceOf[String]
    val cli = conf.get(CatalystContextWrapper.EXEC_MODE, CatalystContextWrapper.EXEC_MODE_SPARK)

    var launcher = candidates.find(_._1 == cli).getOrElse(candidates.head)

//    val promptStr = if (db != null) launcher._1 + db else ""
    
    (launcher._1, launcher._2)
  }
}