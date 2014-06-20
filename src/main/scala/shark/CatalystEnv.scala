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

import scala.collection.mutable
import scala.collection.mutable.{HashMap, HashSet}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.shims.ShimLoader
import org.apache.spark.SparkConf
import org.apache.spark.scheduler.StatsReportListener
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.CatalystContext
import org.apache.spark.scheduler.SplitInfo

/** A singleton object for the master program. The slaves should not access this. */
// TODO add tachyon / memory store based (Copied from SharkEnv.scala)
object CatalystEnv extends LogHelper {

  def init(): CatalystContext = {
    if (catalystContext == null) {
      initWithCatalystContext()
    }

    catalystContext
  }

  def fixIncompatibleConf(conf: Configuration) {
    if (sparkContext == null) {
      init()
    }

    val hiveIsLocal = ShimLoader.getHadoopShims.isLocalMode(conf)
    if (!sparkContext.isLocal && hiveIsLocal) {
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
      master: String = System.getenv("MASTER")): CatalystContext = {

    sparkContext = initSparkContext(jobName, master)
    sparkContext.addSparkListener(new StatsReportListener())

    catalystContext = new CatalystContext(sparkContext)
    catalystContext
  }

  private def initSparkContext(
      jobName: String = "Shark::" + java.net.InetAddress.getLocalHost.getHostName,
      master: String = System.getenv("MASTER")): SparkContext = {

    if (sparkContext != null) {
      sparkContext.stop()
    }

    sparkContext = new SparkContext(
      createSparkConf(if (master == null) "local" else master,
      jobName,
      System.getenv("SPARK_HOME"),
      Nil,
      executorEnvVars), Map[String, Set[SplitInfo]]())

    sparkContext
  }

  private def createSparkConf(
      master: String,
      jobName: String,
      sparkHome: String,
      jars: Seq[String],
      environment: HashMap[String, String]): SparkConf = {

    val newConf = new SparkConf()
      .setMaster(master)
      .setAppName(jobName)
      .setJars(jars)
      .setExecutorEnv(environment.toSeq)

    Option(sparkHome).foreach(newConf.setSparkHome)
    newConf
  }

  logDebug("Initializing SharkEnv")

  val executorEnvVars = {
    val envVars = Set(
      "SPARK_MEM",
      "SPARK_CLASSPATH",
      "HADOOP_HOME",
      "JAVA_HOME",
      "MESOS_NATIVE_LIBRARY",
      "TACHYON_MASTER",
      "TACHYON_WAREHOUSE_PATH")
    HashMap.empty ++= envVars.map { key =>
      key -> Option(System.getenv(key)).getOrElse("")
    }.toMap
  }

  var catalystContext: CatalystContext = _

  var sparkContext: SparkContext = _

  /** Cleans up and shuts down the Shark environments. */
  def stop() {
    logDebug("Shutting down Shark Environment")
    // Stop the SparkContext
    if (CatalystEnv.sparkContext != null) {
      sparkContext.stop()
      sparkContext = null
      catalystContext = null
    }
  }
}
