package shark

import java.io.PrintStream
import java.util.ArrayList

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.Driver
import org.apache.hadoop.hive.ql.session.SessionState
import org.apache.hadoop.hive.ql.processors.CommandProcessorFactory

import scala.collection.JavaConversions._

import shark.execution.TableRDD
import spark.SparkContext


class SharkContext(
    master: String,
    frameworkName: String,
    sparkHome: String = null,
    jars: Seq[String] = Nil)
  extends SparkContext(master, frameworkName, sparkHome, jars) {
  
  val hiveconf = new HiveConf(classOf[SessionState])
  
  //SessionState.initHiveLog4j()
  val sessionState = new SessionState(hiveconf)
  sessionState.out = new PrintStream(System.out, true, "UTF-8")
  sessionState.err = new PrintStream(System.out, true, "UTF-8")

  /**
   * Execute the command and return the results as a sequence. Each element
   * in the sequence is one row.
   */
  def sql(cmd: String): Seq[String] = {

    val cmd_trimmed: String = cmd.trim()
    val tokens: Array[String] = cmd_trimmed.split("\\s+")
    val cmd_1: String = cmd_trimmed.substring(tokens(0).length()).trim()
    val proc = CommandProcessorFactory.get(tokens(0), hiveconf)

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
    SessionState.start(sessionState)
    val driver = new SharkDriver(hiveconf)
    driver.init()
    driver.tableRdd(cmd)
  }

  /**
   * Execute the command and print the results to console.
   */
  def sql2console(cmd: String) {
    val results = sql(cmd)
    results.foreach(println)
  }
}
