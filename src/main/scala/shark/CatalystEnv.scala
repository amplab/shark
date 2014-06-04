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

import scala.collection.mutable.{HashMap, HashSet}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.shims.ShimLoader
import org.apache.spark.SparkConf
import org.apache.spark.scheduler.StatsReportListener
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.CatalystContext

/** A singleton object for the master program. The slaves should not access this. */
// TODO add tachyon / memory store based (Copied from SharkEnv.scala)
object CatalystEnv extends LogHelper {

  def init(): CatalystContext = {
    if (sc == null) {
      initWithCatalystContext()
    }

    cc
  }

  def fixUncompatibleConf(conf: Configuration) {
    if (sc == null) {
      init()
    }

    val hiveIslocal = ShimLoader.getHadoopShims.isLocalMode(conf)
    if (!sc.isLocal && hiveIslocal) {
      val warnMessage = "Hive Hadoop shims detected local mode, but Shark is not running locally."
      logWarning(warnMessage)

      // Try to fix this without bothering user
      val newValue = "Spark_%s".format(System.currentTimeMillis())
      for (k <- Seq("mapred.job.tracker", "mapreduce.framework.name")) {
        val v = conf.get(k)
        if (v == null || v == "" || v == "local") {
          conf.set(k, newValue)
          logWarning("Setting %s to '%s' (was '%s')".format(k, newValue, v))
        }
      }

      // If still not fixed, bail out
      if (ShimLoader.getHadoopShims.isLocalMode(conf)) {
        throw new Exception(warnMessage)
      }
    }
  }

  def initWithCatalystContext(
      jobName: String = "Shark::" + java.net.InetAddress.getLocalHost.getHostName,
      master: String = System.getenv("MASTER"))
    : CatalystContext = {
    sc = initSparkContext(jobName, master)

    sc.addSparkListener(new StatsReportListener())
    
    cc = CatalystContext(sc)
    
    cc
  }

  private def initSparkContext(conf: SparkConf): SparkContext = {
    if (sc != null) {
      sc.stop()
    }

    sc = new SparkContext(conf)
    sc.addSparkListener(new StatsReportListener())

    sc
  }

  private def initSparkContext(
      jobName: String = "Shark::" + java.net.InetAddress.getLocalHost.getHostName,
      master: String = System.getenv("MASTER")): SparkContext = {
    if (sc != null) {
      sc.stop()
    }
    
    sc = new SparkContext(
      if (master == null) "local" else master,
      jobName,
      System.getenv("SPARK_HOME"),
      Nil,
      executorEnvVars)

    sc
  }

  logDebug("Initializing SharkEnv")

  val executorEnvVars = new HashMap[String, String]
  executorEnvVars.put("SPARK_MEM", getEnv("SPARK_MEM"))
  executorEnvVars.put("SPARK_CLASSPATH", getEnv("SPARK_CLASSPATH"))
  executorEnvVars.put("HADOOP_HOME", getEnv("HADOOP_HOME"))
  executorEnvVars.put("JAVA_HOME", getEnv("JAVA_HOME"))
  executorEnvVars.put("MESOS_NATIVE_LIBRARY", getEnv("MESOS_NATIVE_LIBRARY"))
  executorEnvVars.put("TACHYON_MASTER", getEnv("TACHYON_MASTER"))
  executorEnvVars.put("TACHYON_WAREHOUSE_PATH", getEnv("TACHYON_WAREHOUSE_PATH"))

  val activeSessions = new HashSet[String]

  var cc: CatalystContext = _
  var sc: SparkContext = _

  // The following line turns Kryo serialization debug log on. It is extremely chatty.
  //com.esotericsoftware.minlog.Log.set(com.esotericsoftware.minlog.Log.LEVEL_DEBUG)

  // Keeps track of added JARs and files so that we don't add them twice in consecutive queries.
  val addedFiles = HashSet[String]()
  val addedJars = HashSet[String]()

  /** Cleans up and shuts down the Shark environments. */
  def stop() {
    logDebug("Shutting down Shark Environment")
    // Stop the SparkContext
    if (CatalystEnv.sc != null) {
      sc.stop()
      sc = null
      cc = null
    }
  }

  /** Return the value of an environmental variable as a string. */
  def getEnv(varname: String) = if (System.getenv(varname) == null) "" else System.getenv(varname)

}
