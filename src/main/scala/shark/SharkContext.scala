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

import java.io.PrintStream
import java.util.{ArrayList => JArrayList}

import scala.collection.Map
import scala.collection.JavaConversions._

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.common.LogUtils
import org.apache.hadoop.hive.common.LogUtils.LogInitializationException
import org.apache.hadoop.hive.ql.Driver
import org.apache.hadoop.hive.ql.processors.CommandProcessor
import org.apache.hadoop.hive.ql.processors.CommandProcessorFactory
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse
import org.apache.hadoop.hive.ql.session.SessionState

import org.apache.spark.{SparkContext, SparkEnv}

import shark.api._


class SharkContext(
    master: String,
    jobName: String,
    sparkHome: String,
    jars: Seq[String],
    environment: Map[String, String])
  extends SparkContext(master, jobName, sparkHome, jars, environment) {

  @transient val sparkEnv = SparkEnv.get

  SharkContext.init()
  import SharkContext._

  /**
   * Execute the command and return the results as a sequence. Each element
   * in the sequence is one row.
   */
  def sql(cmd: String, maxRows: Int = 1000): Seq[String] = {
    SparkEnv.set(sparkEnv)
    val cmd_trimmed: String = cmd.trim()
    val tokens: Array[String] = cmd_trimmed.split("\\s+")
    val cmd_1: String = cmd_trimmed.substring(tokens(0).length()).trim()
    val proc: CommandProcessor = CommandProcessorFactory.get(tokens(0), hiveconf)

    SessionState.start(sessionState)

    if (proc.isInstanceOf[Driver]) {
      val driver: Driver =
        if (SharkConfVars.getVar(hiveconf, SharkConfVars.EXEC_MODE) == "shark") {
          new SharkDriver(hiveconf)
        } else {
          proc.asInstanceOf[Driver]
        }
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
      results
    } else {
      sessionState.out.println(tokens(0) + " " + cmd_1)
      Seq(proc.run(cmd_1).getResponseCode.toString)
    }
  }

  /**
   * Execute a SQL command and return the results as a TableRDD. The SQL command must be
   * a SELECT statement.
   */
  def sql2rdd(cmd: String): TableRDD = {
    SparkEnv.set(sparkEnv)
    SessionState.start(sessionState)
    val driver = new SharkDriver(hiveconf)
    try {
      driver.init()
      driver.tableRdd(cmd).get
    } finally {
      driver.destroy()
    }
  }

  /**
   * Execute a SQL command and collect the results locally.
   *
   * @param cmd The SQL command to be executed.
   * @param maxRows The max number of rows to retrieve for the result set.
   * @return A ResultSet object with both the schema and the query results.
   */
  def runSql(cmd: String, maxRows: Int = 1000): ResultSet = {
    SparkEnv.set(sparkEnv)

    val cmd_trimmed: String = cmd.trim()
    val tokens: Array[String] = cmd_trimmed.split("\\s+")
    val cmd_1: String = cmd_trimmed.substring(tokens(0).length()).trim()
    val proc: CommandProcessor = CommandProcessorFactory.get(tokens(0), hiveconf)

    SessionState.start(sessionState)

    if (proc.isInstanceOf[Driver]) {
      val driver = new SharkDriver(hiveconf)
      try {
        driver.init()

        driver.tableRdd(cmd) match {
          case Some(rdd) =>
            // If this is a select statement, we will get a TableRDD back. Collect
            // results using that.
            val numCols = rdd.schema.length
            val data = rdd.map { row: Row =>
              Array.tabulate(numCols) { i => row.get(i) }
            }

            if (rdd.limit < 0) {
              new ResultSet(rdd.schema, data.take(maxRows))
            } else {
              new ResultSet(rdd.schema, data.take(math.min(maxRows, rdd.limit)))
            }
          case None =>
            // If this is not a select statement, we use the Driver's getResults function
            // to fetch the results back.
            val schema = ColumnDesc.createSchema(driver.getSchema)
            val results = new JArrayList[String]
            driver.setMaxRows(maxRows)
            driver.getResults(results)
            new ResultSet(schema, results.map(_.split("\t").asInstanceOf[Array[Object]]).toArray)
        }
      } finally {
        driver.destroy()
      }
    } else {
      sessionState.out.println(tokens(0) + " " + cmd_1)
      val response = proc.run(cmd_1)
      new ResultSet(ColumnDesc.createSchema(response.getSchema),
        Array(Array(response.toString : Object)))
    }
  }

  /**
   * Execute the command and print the results to console.
   */
  def sql2console(cmd: String, maxRows: Int = 1000) {
    SparkEnv.set(sparkEnv)
    val results = sql(cmd, maxRows)
    results.foreach(println)
  }
}


object SharkContext {
  // Since we can never properly shut down Hive, we put the Hive related initializations
  // here in a global singleton.

  @transient val hiveconf = new HiveConf(classOf[SessionState])
  Utils.setAwsCredentials(hiveconf)

  try {
    LogUtils.initHiveLog4j()
  } catch {
    case e: LogInitializationException => // Ignore the error.
  }

  @transient val sessionState = new SessionState(hiveconf)
  sessionState.out = new PrintStream(System.out, true, "UTF-8")
  sessionState.err = new PrintStream(System.out, true, "UTF-8")

  // A dummy init to make sure the object is properly initialized.
  def init() {}
}


