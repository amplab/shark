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

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.StatsReportListener

import shark.api.JavaSharkContext
import shark.execution.serialization.{KryoSerializer, ShuffleSerializer}
import shark.memstore2.{MemoryMetadataManager, Table}
import shark.tachyon.TachyonUtilImpl


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
  executorEnvVars.put("SCALA_HOME", getEnv("SCALA_HOME"))
  executorEnvVars.put("SPARK_MEM", getEnv("SPARK_MEM"))
  executorEnvVars.put("SPARK_CLASSPATH", getEnv("SPARK_CLASSPATH"))
  executorEnvVars.put("HADOOP_HOME", getEnv("HADOOP_HOME"))
  executorEnvVars.put("JAVA_HOME", getEnv("JAVA_HOME"))
  executorEnvVars.put("MESOS_NATIVE_LIBRARY", getEnv("MESOS_NATIVE_LIBRARY"))
  executorEnvVars.put("TACHYON_MASTER", getEnv("TACHYON_MASTER"))
  executorEnvVars.put("TACHYON_WAREHOUSE_PATH", getEnv("TACHYON_WAREHOUSE_PATH"))

  val activeSessions = new HashSet[String]

  var sc: SharkContext = _

  val shuffleSerializerName = classOf[ShuffleSerializer].getName

  val memoryMetadataManager = new MemoryMetadataManager

  val tachyonUtil = new TachyonUtilImpl(
    System.getenv("TACHYON_MASTER"), System.getenv("TACHYON_WAREHOUSE_PATH"))

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


/** A singleton object for the slaves. */
object SharkEnvSlave {

  val tachyonUtil = new TachyonUtilImpl(
    System.getenv("TACHYON_MASTER"), System.getenv("TACHYON_WAREHOUSE_PATH"))
}
