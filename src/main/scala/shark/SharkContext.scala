package shark

import java.io.PrintStream
import java.util.ArrayList

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.session.SessionState

import scala.collection.JavaConversions._

import shark.exec.TableRDD
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
  
  val driver = new SharkDriver(hiveconf)
  driver.init()

  /**
   * Execute the command and return the results as a sequence. Each element
   * in the sequence is one row.
   */
  def sql(cmd: String): Seq[String] = {
    val results = new ArrayList[String]()
    SessionState.start(sessionState)
    driver.run(cmd)
    driver.getResults(results)
    results
  }
  
  /**
   * Execute the command and return the results as a TableRDD.
   */
  def sql2rdd(cmd: String): TableRDD = {
    SessionState.start(sessionState)
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
