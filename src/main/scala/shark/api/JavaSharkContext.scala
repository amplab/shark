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

package shark.api

import java.util.{Map => JMap}
import java.util.{List => JList}

import scala.collection.JavaConversions._

import org.apache.spark.api.java.JavaSparkContext

import shark.SharkContext


class JavaSharkContext(val sharkCtx: SharkContext) extends JavaSparkContext(sharkCtx) {

  /**
   * @param master Cluster URL to connect to (e.g. mesos://host:port, spark://host:port, local[4]).
   * @param jobName A name for your job, to display on the cluster web UI
   */
  def this(master: String, jobName: String) =
    this(new SharkContext(master, jobName, null, Nil, Map.empty))

  /**
   * @param master Cluster URL to connect to (e.g. mesos://host:port, spark://host:port, local[4]).
   * @param jobName A name for your job, to display on the cluster web UI
   * @param sparkHome The SPARK_HOME directory on the slave nodes
   * @param jarFile A JAR to send to the cluster. This can be a path on the local file
   *                system or a HDFS, HTTP, HTTPS, or FTP URL.
   */
  def this(master: String, jobName: String, sparkHome: String, jarFile: String) =
    this(new SharkContext(master, jobName, sparkHome, Seq(jarFile), Map.empty))

  /**
   * @param master Cluster URL to connect to (e.g. mesos://host:port, spark://host:port, local[4]).
   * @param jobName A name for your job, to display on the cluster web UI
   * @param sparkHome The SPARK_HOME directory on the slave nodes
   * @param jars Collection of JARs to send to the cluster. These can be paths on the local file
   *             system or HDFS, HTTP, HTTPS, or FTP URLs.
   */
  def this(master: String, jobName: String, sparkHome: String, jars: Array[String]) =
    this(new SharkContext(master, jobName, sparkHome, jars.toSeq, Map.empty))

  /**
   * @param master Cluster URL to connect to (e.g. mesos://host:port, spark://host:port, local[4]).
   * @param jobName A name for your job, to display on the cluster web UI
   * @param sparkHome The SPARK_HOME directory on the slave nodes
   * @param jars Collection of JARs to send to the cluster. These can be paths on the local file
   *             system or HDFS, HTTP, HTTPS, or FTP URLs.
   * @param environment Environment variables to set on worker nodes
   */
  def this(master: String, jobName: String, sparkHome: String, jars: Array[String],
           environment: JMap[String, String]) =
    this(new SharkContext(master, jobName, sparkHome, jars.toSeq, environment))

  /**
   * Execute the command and return the results as a sequence. Each element
   * in the sequence is one row.
   */
  def sql(cmd: String): JList[String] = sharkCtx.sql(cmd)

  /**
   * Execute the command and return the results as a TableRDD.
   */
  def sql2rdd(cmd: String): JavaTableRDD = {
    val rdd = sharkCtx.sql2rdd(cmd)
    new JavaTableRDD(rdd, rdd.schema)
  }


  /**
   * Execute a SQL command and collect the results locally. This function returns a maximum of
   * 1000 rows. To fetch a larger result set, use runSql with maxRows specified.
   *
   * @param cmd The SQL command to be executed.
   * @return A ResultSet object with both the schema and the query results.
   */
  def runSql(cmd: String): ResultSet = sharkCtx.runSql(cmd)

  /**
   * Execute a SQL command and collect the results locally.
   *
   * @param cmd The SQL command to be executed.
   * @param maxRows The max number of rows to retrieve for the result set.
   * @return A ResultSet object with both the schema and the query results.
   */
  def runSql(cmd: String, maxRows: Int) = sharkCtx.runSql(cmd, maxRows)

  /**
   * Execute the command and print the results to the console.
   */
  def sql2console(cmd: String) {
    sharkCtx.sql2console(cmd)
  }
}
