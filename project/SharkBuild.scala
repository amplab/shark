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

import sbt._
import Keys._

import sbtassembly.Plugin._
import AssemblyKeys._

import scala.util.Properties.{ envOrNone => env }

object SharkBuild extends Build {

  // Shark version
  val SHARK_VERSION = "0.9.0-SNAPSHOT"

  val SPARK_VERSION = "0.9.0-incubating-SNAPSHOT"

  val SCALA_VERSION = "2.9.3"

  // Hadoop version to build against. For example, "0.20.2", "0.20.205.0", or
  // "1.0.1" for Apache releases, or "0.20.2-cdh3u3" for Cloudera Hadoop.
  val DEFAULT_HADOOP_VERSION = "1.0.4"

  lazy val hadoopVersion = env("SHARK_HADOOP_VERSION") orElse
                           env("SPARK_HADOOP_VERSION") getOrElse
                           DEFAULT_HADOOP_VERSION

  // Whether to build Shark with Yarn support
  val YARN_ENABLED = env("SHARK_YARN").getOrElse("false").toBoolean

  // Whether to build Shark with Tachyon jar.
  val TACHYON_ENABLED = true

  lazy val root = Project(
    id = "root",
    base = file("."),
    settings = coreSettings ++ assemblyProjSettings)

  val excludeKyro = ExclusionRule(organization = "de.javakaffee")
  val excludeHadoop = ExclusionRule(organization = "org.apache.hadoop")
  val excludeNetty = ExclusionRule(organization = "org.jboss.netty")
  val excludeCurator = ExclusionRule(organization = "org.apache.curator")
  val excludeJackson = ExclusionRule(organization = "org.codehaus.jackson")
  val excludeAsm = ExclusionRule(organization = "asm")
  val excludeSnappy = ExclusionRule(organization = "org.xerial.snappy")

  def coreSettings = Defaults.defaultSettings ++ Seq(

    name := "shark",
    organization := "edu.berkeley.cs.amplab",
    version := SHARK_VERSION,
    scalaVersion := SCALA_VERSION,
    scalacOptions := Seq("-deprecation", "-unchecked", "-optimize"),
    parallelExecution in Test := false,

    // Download managed jars into lib_managed.
    retrieveManaged := true,
    resolvers ++= Seq(
      "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
      "Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
      "Local Maven" at Path.userHome.asFile.toURI.toURL + ".m2/repository"
    ),

    fork := true,
    javaOptions += "-XX:MaxPermSize=512m",
    javaOptions += "-Xmx2g",

    testOptions in Test += Tests.Argument("-oF"), // Full stack trace on test failures

    testListeners <<= target.map(
      t => Seq(new eu.henkelmann.sbt.JUnitXmlTestsListener(t.getAbsolutePath))),

    unmanagedSourceDirectories in Compile <+= baseDirectory { base =>
      if (TACHYON_ENABLED) {
        base / ("src/tachyon_enabled/scala")
      } else {
        base / ("src/tachyon_disabled/scala")
      }
    },

    unmanagedJars in Compile <++= baseDirectory map { base =>
      val hiveFile = file(System.getenv("HIVE_HOME")) / "lib"
      val baseDirectories = (base / "lib") +++ (hiveFile)
      val customJars = (baseDirectories ** "*.jar")
      // Hive uses an old version of guava that doesn't have what we want.
      customJars.classpath
        .filter(!_.toString.contains("guava"))
        .filter(!_.toString.contains("log4j"))
        .filter(!_.toString.contains("servlet"))
    },

    unmanagedJars in Test ++= Seq(
      file(System.getenv("HIVE_DEV_HOME")) / "build" / "ql" / "test" / "classes",
      file(System.getenv("HIVE_DEV_HOME")) / "build/ivy/lib/test/hadoop-test-0.20.2.jar"
    ),

    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % SPARK_VERSION,
      "org.apache.spark" %% "spark-repl" % SPARK_VERSION,
      "com.google.guava" % "guava" % "14.0.1",
      "org.apache.hadoop" % "hadoop-client" % hadoopVersion excludeAll(excludeJackson, excludeNetty, excludeAsm) force(),
      // See https://code.google.com/p/guava-libraries/issues/detail?id=1095
      "com.google.code.findbugs" % "jsr305" % "1.3.+",

      // Hive unit test requirements. These are used by Hadoop to run the tests, but not necessary
      // in usual Shark runs.
      "commons-io" % "commons-io" % "2.1",
      "commons-httpclient" % "commons-httpclient" % "3.1" % "test",

      // Test infrastructure
      "org.scalatest" %% "scalatest" % "1.9.1" % "test",
      "junit" % "junit" % "4.10" % "test",
      "net.java.dev.jets3t" % "jets3t" % "0.7.1",
      "com.novocode" % "junit-interface" % "0.8" % "test") ++
      (if (YARN_ENABLED) Some("org.apache.spark" %% "spark-yarn" % SPARK_VERSION) else None).toSeq ++
      (if (TACHYON_ENABLED) Some("org.tachyonproject" % "tachyon" % "0.3.0" excludeAll(excludeKyro, excludeHadoop, excludeCurator, excludeJackson, excludeNetty, excludeAsm)) else None).toSeq
  ) ++ org.scalastyle.sbt.ScalastylePlugin.Settings

  def assemblyProjSettings = Seq(
    jarName in assembly <<= version map { v => "shark-assembly-" + v + "-hadoop" + hadoopVersion + ".jar" }
  ) ++ assemblySettings ++ extraAssemblySettings

  def extraAssemblySettings() = Seq(
    test in assembly := {},
    mergeStrategy in assembly := {
      case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
      case m if m.toLowerCase.matches("meta-inf.*\\.sf$") => MergeStrategy.discard
      case "META-INF/services/org.apache.hadoop.fs.FileSystem" => MergeStrategy.concat
      case "reference.conf" => MergeStrategy.concat
      case _ => MergeStrategy.first
    }
  )
}
