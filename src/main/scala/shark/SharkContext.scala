package shark

import java.io.PrintStream
import java.util.ArrayList

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.Driver
import org.apache.hadoop.hive.ql.processors.{CommandProcessor, CommandProcessorFactory}
import org.apache.hadoop.hive.ql.session.SessionState

import scala.collection.Map
import scala.collection.JavaConversions._

import shark.execution.TableRDD
import spark.{SparkContext, SparkEnv}


class SharkContext(
    master: String,
    jobName: String,
    sparkHome: String,
    jars: Seq[String],
    environment: Map[String, String])
  extends SparkContext(master, jobName, sparkHome, jars, environment) {

  @transient val sparkEnv = SparkEnv.get

  @transient val hiveconf = new HiveConf(classOf[SessionState])

  //SessionState.initHiveLog4j()
  @transient val sessionState = new SessionState(hiveconf)
  sessionState.out = new PrintStream(System.out, true, "UTF-8")
  sessionState.err = new PrintStream(System.out, true, "UTF-8")

  /**
   * Execute the command and return the results as a sequence. Each element
   * in the sequence is one row.
   */
  def sql(cmd: String): Seq[String] = {
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

      val results = new ArrayList[String]()
      driver.run(cmd)
      driver.getResults(results)
      driver.destroy()
      results
    } else {
      sessionState.out.println(tokens(0) + " " + cmd_1)
      Seq(proc.run(cmd_1).getResponseCode().toString)
    }
  }

  /**
   * Execute the command and return the results as a TableRDD.
   */
  def sql2rdd(cmd: String): TableRDD = {
    SparkEnv.set(sparkEnv)
    SessionState.start(sessionState)
    val driver = new SharkDriver(hiveconf)
    driver.init()
    driver.tableRdd(cmd)
  }

  /**
   * Execute the command and print the results to console.
   */
  def sql2console(cmd: String) {
    SparkEnv.set(sparkEnv)
    val results = sql(cmd)
    results.foreach(println)
  }
}
