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

import shark.api.JavaSharkContext
import shark.execution.serialization.ShuffleSerializer
import shark.memstore2.{MemoryMetadataManager, OffHeapStorageClient}

/** A singleton object for the master program. The slaves should not access this. */
object SharkEnv extends LogHelper {

  def init(): SharkContext = {
    if (sc == null) {
      val jobName = "Shark::" + java.net.InetAddress.getLocalHost.getHostName
      val master = System.getenv("MASTER")
      initWithSharkContext(jobName, master)
    }
    sc
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

  def initWithSharkContext(
      jobName: String = "Shark::" + java.net.InetAddress.getLocalHost.getHostName,
      master: String = System.getenv("MASTER"))
    : SharkContext = {
    if (sc != null) {
      sc.stop()
    }

    sc = new SharkContext(
      if (master == null) "local" else master,
      jobName,
      System.getenv("SPARK_HOME"),
      Nil,
      executorEnvVars)
    sc.addSparkListener(new StatsReportListener())
    sc
  }

  def initWithSharkContext(conf: SparkConf): SharkContext = {
    conf.setExecutorEnv(executorEnvVars.toSeq)
    initWithSharkContext(new SharkContext(conf))
  }

  def initWithSharkContext(newSc: SharkContext): SharkContext = {
    if (sc != null) {
      sc.stop()
    }
    sc = newSc
    sc.addSparkListener(new StatsReportListener())
    sc
  }

  def initWithJavaSharkContext(jobName: String): JavaSharkContext = {
    new JavaSharkContext(initWithSharkContext(jobName))
  }

  def initWithJavaSharkContext(jobName: String, master: String): JavaSharkContext = {
    new JavaSharkContext(initWithSharkContext(jobName, master))
  }

  def initWithJavaSharkContext(newSc: JavaSharkContext): JavaSharkContext = {
    new JavaSharkContext(initWithSharkContext(newSc.sharkCtx))
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

  // Used to propagate the shark.offheap.clientFactory to executors
  executorEnvVars.put("SHARK_OFFHEAP_CLIENT_FACTORY", OffHeapStorageClient.clientFactoryClassName)

  val activeSessions = new HashSet[String]

  var sc: SharkContext = _

  val shuffleSerializerName = classOf[ShuffleSerializer].getName

  val memoryMetadataManager = new MemoryMetadataManager

  // The following line turns Kryo serialization debug log on. It is extremely chatty.
  //com.esotericsoftware.minlog.Log.set(com.esotericsoftware.minlog.Log.LEVEL_DEBUG)

  // Keeps track of added JARs and files so that we don't add them twice in consecutive queries.
  val addedFiles = HashSet[String]()
  val addedJars = HashSet[String]()

  /** Cleans up and shuts down the Shark environments. */
  def stop() {
    logDebug("Shutting down Shark Environment")
    memoryMetadataManager.shutdown()
    // Stop the SparkContext
    if (SharkEnv.sc != null) {
      sc.stop()
      sc = null
    }
  }

  /** Return the value of an environmental variable as a string. */
  def getEnv(varname: String) = if (System.getenv(varname) == null) "" else System.getenv(varname)

}
