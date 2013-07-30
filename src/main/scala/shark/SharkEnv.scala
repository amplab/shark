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

import shark.api.JavaSharkContext
import shark.memstore2.MemoryMetadataManager
import shark.tachyon.TachyonUtilImpl
import spark.{RDD, SparkContext}
import spark.scheduler.StatsReportListener

/** A singleton object for the master program. The slaves should not access this. */
object SharkEnv extends LogHelper {

  def init(): SparkContext = {
    if (sc == null) {
      sc = new SparkContext(
          if (System.getenv("MASTER") == null) "local" else System.getenv("MASTER"),
          "Shark::" + java.net.InetAddress.getLocalHost.getHostName,
          System.getenv("SPARK_HOME"),
          Nil,
          executorEnvVars)
      sc.addSparkListener(new StatsReportListener())
    }
    sc
  }

  def initWithSharkContext(jobName: String, master: String = System.getenv("MASTER"))
    : SharkContext = {
    if (sc != null) {
      sc.stop
    }

    sc = new SharkContext(
        if (master == null) "local" else master,
        jobName,
        System.getenv("SPARK_HOME"),
        Nil,
        executorEnvVars)
    sc.addSparkListener(new StatsReportListener())
    sc.asInstanceOf[SharkContext]
  }

  def initWithSharkContext(newSc: SharkContext): SharkContext = {
    if (sc != null) {
      sc.stop
    }

    sc = newSc
    sc.asInstanceOf[SharkContext]
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

  logInfo("Initializing SharkEnv")

  System.setProperty("spark.serializer", classOf[spark.KryoSerializer].getName)
  System.setProperty("spark.kryo.registrator", classOf[KryoRegistrator].getName)

  val executorEnvVars = new HashMap[String, String]
  executorEnvVars.put("SCALA_HOME", getEnv("SCALA_HOME"))
  executorEnvVars.put("SPARK_MEM", getEnv("SPARK_MEM"))
  executorEnvVars.put("SPARK_CLASSPATH", getEnv("SPARK_CLASSPATH"))
  executorEnvVars.put("HADOOP_HOME", getEnv("HADOOP_HOME"))
  executorEnvVars.put("JAVA_HOME", getEnv("JAVA_HOME"))
  executorEnvVars.put("MESOS_NATIVE_LIBRARY", getEnv("MESOS_NATIVE_LIBRARY"))
  executorEnvVars.put("TACHYON_MASTER", getEnv("TACHYON_MASTER"))
  executorEnvVars.put("TACHYON_WAREHOUSE_PATH", getEnv("TACHYON_WAREHOUSE_PATH"))

  var sc: SparkContext = _

  val memoryMetadataManager = new MemoryMetadataManager

  val tachyonUtil = new TachyonUtilImpl(
    System.getenv("TACHYON_MASTER"), System.getenv("TACHYON_WAREHOUSE_PATH"))

  // The following line turns Kryo serialization debug log on. It is extremely chatty.
  //com.esotericsoftware.minlog.Log.set(com.esotericsoftware.minlog.Log.LEVEL_DEBUG)

  // Keeps track of added JARs and files so that we don't add them twice in consecutive queries.
  val addedFiles = HashSet[String]()
  val addedJars = HashSet[String]()

  def unpersist(key: String): Option[RDD[_]] = {
    memoryMetadataManager.unpersist(key)
  }

  /** Cleans up and shuts down the Shark environments. */
  def stop() {
    logInfo("Shutting down Shark Environment")
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
  /**
   * A lock for various operations in ObjectInspectorFactory. Methods in that
   * class uses a static objectInspectorCache object to cache the creation of
   * object inspectors. That object is not thread safe so we wrap all calls to
   * that object in a synchronized lock on this.
   */
  val objectInspectorLock: AnyRef = new Object()

  val tachyonUtil = new TachyonUtilImpl(
    System.getenv("TACHYON_MASTER"), System.getenv("TACHYON_WAREHOUSE_PATH"))
}
