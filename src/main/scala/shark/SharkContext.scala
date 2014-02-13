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
import scala.reflect.ClassTag

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.Driver
import org.apache.hadoop.hive.ql.processors.CommandProcessor
import org.apache.hadoop.hive.ql.processors.CommandProcessorFactory
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse
import org.apache.hadoop.hive.ql.session.SessionState

import org.apache.spark.{SparkConf, SparkContext, SparkEnv}
import org.apache.spark.scheduler.SplitInfo
import org.apache.spark.rdd.RDD

import shark.api._
import shark.tgf.TGF


class SharkContext(
    config: SparkConf,
    preferredNodeLocationData: Map[String, Set[SplitInfo]] = Map())
  extends SparkContext(config, preferredNodeLocationData) {
  import SharkContext._

  @transient val sparkEnv = SparkEnv.get

  private type C[T] = ClassTag[T]
  private def ct[T](implicit c : ClassTag[T]) = c

  SharkContext.init()

  def this(
      master: String,
      jobName: String,
      sparkHome: String,
      jars: Seq[String],
      environment: Map[String, String]) {
    this(SharkContext.createSparkConf(master, jobName, sparkHome, jars, environment))
  }

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
   * Execute a SQL command and return the results as a RDD of Seq. The SQL command must be
   * a SELECT statement. This is useful if the table has more than 22 columns (more than fits in tuples)
   * NB: These are auto-generated using resources/tablerdd/SharkContext_sqlRdd_generator.py
   */
  def sqlSeqRdd(cmd: String): RDD[Seq[Any]] = {
    new TableSeqRDD(sql2rdd(cmd))
  }

  /**
   * Execute a SQL command and return the results as a RDD of Tuple. The SQL command must be
   * a SELECT statement.
   */

  def sqlRdd[T1: C, T2: C](cmd: String):
  RDD[Tuple2[T1, T2]] = {
    new TableRDD2[T1, T2](sql2rdd(cmd),
      Seq(ct[T1], ct[T2]))
  }

  def sqlRdd[T1: C, T2: C, T3: C](cmd: String):
  RDD[Tuple3[T1, T2, T3]] = {
    new TableRDD3[T1, T2, T3](sql2rdd(cmd),
      Seq(ct[T1], ct[T2], ct[T3]))
  }

  def sqlRdd[T1: C, T2: C, T3: C, T4: C](cmd: String):
  RDD[Tuple4[T1, T2, T3, T4]] = {
    new TableRDD4[T1, T2, T3, T4](sql2rdd(cmd),
      Seq(ct[T1], ct[T2], ct[T3], ct[T4]))
  }

  def sqlRdd[T1: C, T2: C, T3: C, T4: C, T5: C](cmd: String):
  RDD[Tuple5[T1, T2, T3, T4, T5]] = {
    new TableRDD5[T1, T2, T3, T4, T5](sql2rdd(cmd),
      Seq(ct[T1], ct[T2], ct[T3], ct[T4], ct[T5]))
  }

  def sqlRdd[T1: C, T2: C, T3: C, T4: C, T5: C, T6: C](cmd: String):
  RDD[Tuple6[T1, T2, T3, T4, T5, T6]] = {
    new TableRDD6[T1, T2, T3, T4, T5, T6](sql2rdd(cmd),
      Seq(ct[T1], ct[T2], ct[T3], ct[T4], ct[T5], ct[T6]))
  }

  def sqlRdd[T1: C, T2: C, T3: C, T4: C, T5: C, T6: C, T7: C](cmd: String):
  RDD[Tuple7[T1, T2, T3, T4, T5, T6, T7]] = {
    new TableRDD7[T1, T2, T3, T4, T5, T6, T7](sql2rdd(cmd),
      Seq(ct[T1], ct[T2], ct[T3], ct[T4], ct[T5], ct[T6], ct[T7]))
  }

  def sqlRdd[T1: C, T2: C, T3: C, T4: C, T5: C, T6: C, T7: C, T8: C](cmd: String):
  RDD[Tuple8[T1, T2, T3, T4, T5, T6, T7, T8]] = {
    new TableRDD8[T1, T2, T3, T4, T5, T6, T7, T8](sql2rdd(cmd),
      Seq(ct[T1], ct[T2], ct[T3], ct[T4], ct[T5], ct[T6], ct[T7], ct[T8]))
  }

  def sqlRdd[T1: C, T2: C, T3: C, T4: C, T5: C, T6: C, T7: C, T8: C, T9: C](cmd: String):
  RDD[Tuple9[T1, T2, T3, T4, T5, T6, T7, T8, T9]] = {
    new TableRDD9[T1, T2, T3, T4, T5, T6, T7, T8, T9](sql2rdd(cmd),
      Seq(ct[T1], ct[T2], ct[T3], ct[T4], ct[T5], ct[T6], ct[T7], ct[T8], ct[T9]))
  }

  def sqlRdd[T1: C, T2: C, T3: C, T4: C, T5: C, T6: C, T7: C, T8: C, T9: C, T10: C](cmd: String):
  RDD[Tuple10[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10]] = {
    new TableRDD10[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10](sql2rdd(cmd),
      Seq(ct[T1], ct[T2], ct[T3], ct[T4], ct[T5], ct[T6], ct[T7], ct[T8], ct[T9], ct[T10]))
  }

  def sqlRdd[T1: C, T2: C, T3: C, T4: C, T5: C, T6: C, T7: C, T8: C, T9: C, T10: C, T11: C](cmd: String):
  RDD[Tuple11[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11]] = {
    new TableRDD11[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11](sql2rdd(cmd),
      Seq(ct[T1], ct[T2], ct[T3], ct[T4], ct[T5], ct[T6], ct[T7], ct[T8], ct[T9], ct[T10], ct[T11]))
  }

  def sqlRdd[T1: C, T2: C, T3: C, T4: C, T5: C, T6: C, T7: C, T8: C, T9: C, T10: C, T11: C, T12: C](cmd: String):
  RDD[Tuple12[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12]] = {
    new TableRDD12[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12](sql2rdd(cmd),
      Seq(ct[T1], ct[T2], ct[T3], ct[T4], ct[T5], ct[T6], ct[T7], ct[T8], ct[T9], ct[T10], ct[T11], ct[T12]))
  }

  def sqlRdd[T1: C, T2: C, T3: C, T4: C, T5: C, T6: C, T7: C, T8: C, T9: C, T10: C, T11: C, T12: C,
  T13: C](cmd: String):
  RDD[Tuple13[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13]] = {
    new TableRDD13[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13](sql2rdd(cmd),
      Seq(ct[T1], ct[T2], ct[T3], ct[T4], ct[T5], ct[T6], ct[T7], ct[T8], ct[T9], ct[T10], ct[T11], ct[T12],
        ct[T13]))
  }

  def sqlRdd[T1: C, T2: C, T3: C, T4: C, T5: C, T6: C, T7: C, T8: C, T9: C, T10: C, T11: C, T12: C,
  T13: C, T14: C](cmd: String):
  RDD[Tuple14[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14]] = {
    new TableRDD14[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14](sql2rdd(cmd),
      Seq(ct[T1], ct[T2], ct[T3], ct[T4], ct[T5], ct[T6], ct[T7], ct[T8], ct[T9], ct[T10], ct[T11], ct[T12],
        ct[T13], ct[T14]))
  }

  def sqlRdd[T1: C, T2: C, T3: C, T4: C, T5: C, T6: C, T7: C, T8: C, T9: C, T10: C, T11: C, T12: C,
  T13: C, T14: C, T15: C](cmd: String):
  RDD[Tuple15[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15]] = {
    new TableRDD15[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15](sql2rdd(cmd),
      Seq(ct[T1], ct[T2], ct[T3], ct[T4], ct[T5], ct[T6], ct[T7], ct[T8], ct[T9], ct[T10], ct[T11], ct[T12],
        ct[T13], ct[T14], ct[T15]))
  }

  def sqlRdd[T1: C, T2: C, T3: C, T4: C, T5: C, T6: C, T7: C, T8: C, T9: C, T10: C, T11: C, T12: C,
  T13: C, T14: C, T15: C, T16: C](cmd: String):
  RDD[Tuple16[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16]] = {
    new TableRDD16[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16](sql2rdd(cmd),
      Seq(ct[T1], ct[T2], ct[T3], ct[T4], ct[T5], ct[T6], ct[T7], ct[T8], ct[T9], ct[T10], ct[T11], ct[T12],
        ct[T13], ct[T14], ct[T15], ct[T16]))
  }

  def sqlRdd[T1: C, T2: C, T3: C, T4: C, T5: C, T6: C, T7: C, T8: C, T9: C, T10: C, T11: C, T12: C,
  T13: C, T14: C, T15: C, T16: C, T17: C](cmd: String):
  RDD[Tuple17[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17]] = {
    new TableRDD17[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17](sql2rdd(cmd),
      Seq(ct[T1], ct[T2], ct[T3], ct[T4], ct[T5], ct[T6], ct[T7], ct[T8], ct[T9], ct[T10], ct[T11], ct[T12],
        ct[T13], ct[T14], ct[T15], ct[T16], ct[T17]))
  }

  def sqlRdd[T1: C, T2: C, T3: C, T4: C, T5: C, T6: C, T7: C, T8: C, T9: C, T10: C, T11: C, T12: C,
  T13: C, T14: C, T15: C, T16: C, T17: C, T18: C](cmd: String):
  RDD[Tuple18[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18]] = {
    new TableRDD18[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18](sql2rdd(cmd),
      Seq(ct[T1], ct[T2], ct[T3], ct[T4], ct[T5], ct[T6], ct[T7], ct[T8], ct[T9], ct[T10], ct[T11], ct[T12],
        ct[T13], ct[T14], ct[T15], ct[T16], ct[T17], ct[T18]))
  }

  def sqlRdd[T1: C, T2: C, T3: C, T4: C, T5: C, T6: C, T7: C, T8: C, T9: C, T10: C, T11: C, T12: C,
  T13: C, T14: C, T15: C, T16: C, T17: C, T18: C, T19: C](cmd: String):
  RDD[Tuple19[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18,
    T19]] = {
    new TableRDD19[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18,
      T19](sql2rdd(cmd),
      Seq(ct[T1], ct[T2], ct[T3], ct[T4], ct[T5], ct[T6], ct[T7], ct[T8], ct[T9], ct[T10], ct[T11], ct[T12],
        ct[T13], ct[T14], ct[T15], ct[T16], ct[T17], ct[T18], ct[T19]))
  }

  def sqlRdd[T1: C, T2: C, T3: C, T4: C, T5: C, T6: C, T7: C, T8: C, T9: C, T10: C, T11: C, T12: C,
  T13: C, T14: C, T15: C, T16: C, T17: C, T18: C, T19: C, T20: C](cmd: String):
  RDD[Tuple20[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18,
    T19, T20]] = {
    new TableRDD20[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18,
      T19, T20](sql2rdd(cmd),
      Seq(ct[T1], ct[T2], ct[T3], ct[T4], ct[T5], ct[T6], ct[T7], ct[T8], ct[T9], ct[T10], ct[T11], ct[T12],
        ct[T13], ct[T14], ct[T15], ct[T16], ct[T17], ct[T18], ct[T19], ct[T20]))
  }

  def sqlRdd[T1: C, T2: C, T3: C, T4: C, T5: C, T6: C, T7: C, T8: C, T9: C, T10: C, T11: C, T12: C,
  T13: C, T14: C, T15: C, T16: C, T17: C, T18: C, T19: C, T20: C, T21: C](cmd: String):
  RDD[Tuple21[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18,
    T19, T20, T21]] = {
    new TableRDD21[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18,
      T19, T20, T21](sql2rdd(cmd),
      Seq(ct[T1], ct[T2], ct[T3], ct[T4], ct[T5], ct[T6], ct[T7], ct[T8], ct[T9], ct[T10], ct[T11], ct[T12],
        ct[T13], ct[T14], ct[T15], ct[T16], ct[T17], ct[T18], ct[T19], ct[T20], ct[T21]))
  }

  def sqlRdd[T1: C, T2: C, T3: C, T4: C, T5: C, T6: C, T7: C, T8: C, T9: C, T10: C, T11: C, T12: C,
  T13: C, T14: C, T15: C, T16: C, T17: C, T18: C, T19: C, T20: C, T21: C, T22: C](cmd: String):
  RDD[Tuple22[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18,
    T19, T20, T21, T22]] = {
    new TableRDD22[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18,
      T19, T20, T21, T22](sql2rdd(cmd),
      Seq(ct[T1], ct[T2], ct[T3], ct[T4], ct[T5], ct[T6], ct[T7], ct[T8], ct[T9], ct[T10], ct[T11], ct[T12],
        ct[T13], ct[T14], ct[T15], ct[T16], ct[T17], ct[T18], ct[T19], ct[T20], ct[T21], ct[T22]))
  }

  /**
   * Execute a SQL command and collect the results locally.
   *
   * @param cmd The SQL command to be executed.
   * @param maxRows The max number of rows to retrieve for the result set.
   * @return A ResultSet object with both the schema and the query results.
   */
  def runSql(cmd: String, maxRows: Int = 1000): ResultSet = {
    if (cmd.trim.toLowerCase().startsWith("generate")) {
      return TGF.execute(cmd.trim, this)
    }

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

  @transient val sessionState = new SessionState(hiveconf)
  sessionState.out = new PrintStream(System.out, true, "UTF-8")
  sessionState.err = new PrintStream(System.out, true, "UTF-8")

  // A dummy init to make sure the object is properly initialized.
  def init() {}

  def createSparkConf(
      master: String,
      jobName: String,
      sparkHome: String,
      jars: Seq[String],
      environment: Map[String, String]): SparkConf = {
    val newConf = new SparkConf()
      .setMaster(master)
      .setAppName(jobName)
      .setJars(jars)
      .setExecutorEnv(environment.toSeq)
    Option(sparkHome).foreach(newConf.setSparkHome(_))
    newConf.set("spark.kryo.registrator", classOf[KryoRegistrator].getName)
    newConf
  }
}
