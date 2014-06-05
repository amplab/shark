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

import com.typesafe.sbt.pgp.PgpKeys._
import scala.util.Properties.{ envOrNone => env }

import net.virtualvoid.sbt.graph.{Plugin => DependencyGraphPlugin}
import sbtassembly.Plugin._
import sbtassembly.Plugin.AssemblyKeys._

object SharkBuild extends Build {

  // Shark version
  val SHARK_VERSION = "1.0.0-PREVIEW-SNAPSHOT"

  val SHARK_ORGANIZATION = "edu.berkeley.cs.shark"

  val SPARK_VERSION = "1.1.0-SNAPSHOT"

  val SCALA_VERSION = "2.10.4"

  val SCALAC_JVM_VERSION = "jvm-1.6"
  val JAVAC_JVM_VERSION = "1.6"
  val JETTY_VERSION = "8.1.14.v20131031"

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
  val TACHYON_VERSION = "0.4.0"

  lazy val root = Project(
    id = "root",
    base = file("."),
    settings = coreSettings ++ assemblyProjSettings)

  val excludeKyro = ExclusionRule(organization = "de.javakaffee")
  val excludeHadoop = ExclusionRule(organization = "org.apache.hadoop")
  val excludeNetty = ExclusionRule(organization = "org.jboss.netty")
  val excludeCurator = ExclusionRule(organization = "org.apache.curator")
  val excludeAsm = ExclusionRule(organization = "asm")
  val excludeSnappy = ExclusionRule(organization = "org.xerial.snappy")
  // Differences in Jackson version cause runtime errors as per HIVE-3581
  val excludeJackson = ExclusionRule(organization = "org.codehaus.jackson")

  // Exclusion rules for Hive artifacts
  val excludeGuava = ExclusionRule(organization = "com.google.guava")
  val excludeLog4j = ExclusionRule(organization = "log4j")
  val excludeServlet = ExclusionRule(organization = "org.mortbay.jetty")
  val excludeXerces = ExclusionRule(organization = "xerces")
  val excludeHive = ExclusionRule(organization = "org.apache.hive")


  /** Extra artifacts not included in Spark SQL's Hive support. */
  val hiveArtifacts = Seq("hive-cli", "hive-jdbc", "hive-exec", "hive-service")
  val hiveDependencies = hiveArtifacts.map ( artifactId =>
    "org.spark-project.hive" % artifactId % "0.12.0" excludeAll(
       excludeGuava, excludeLog4j, excludeServlet, excludeAsm, excludeNetty, excludeXerces)
  )

  val yarnDependency = (if (YARN_ENABLED) {
    Some("org.apache.spark" %% "spark-yarn" % SPARK_VERSION)
  } else {
    None
  }).toSeq

  def coreSettings = Defaults.defaultSettings ++ DependencyGraphPlugin.graphSettings ++ Seq(
    name := "shark",
    organization := SHARK_ORGANIZATION,
    version := SHARK_VERSION,
    scalaVersion := SCALA_VERSION,
    scalacOptions := Seq("-deprecation", "-unchecked", "-optimize", "-feature",
      "-Yinline-warnings", "-target:" + SCALAC_JVM_VERSION),
    javacOptions := Seq("-target", JAVAC_JVM_VERSION, "-source", JAVAC_JVM_VERSION),
    parallelExecution in Test := false,

    libraryDependencies ++= hiveDependencies ++ yarnDependency,
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-hive" % SPARK_VERSION,
      "org.apache.spark" %% "spark-repl" % SPARK_VERSION,
      "org.apache.hadoop" % "hadoop-client" % hadoopVersion excludeAll(excludeJackson, excludeNetty, excludeAsm) force(),
      "com.typesafe" %% "scalalogging-slf4j" % "1.0.1",
      "org.scalatest"    %% "scalatest"       % "1.9.1"  % "test"
    ),

    // Download managed jars into lib_managed.
    retrieveManaged := true,
    resolvers ++= Seq(
      "Local Maven" at Path.userHome.asFile.toURI.toURL + ".m2/repository",
      "Maven Repository"     at "http://repo.maven.apache.org/maven2",
      "Apache Repository"    at "https://repository.apache.org/content/repositories/releases",
      "JBoss Repository"     at "https://repository.jboss.org/nexus/content/repositories/releases/",
      "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
      "Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/"
    ),
 
    publishTo <<= version { (v: String) =>
      val nexus = "https://oss.sonatype.org/"
      if (v.trim.endsWith("SNAPSHOT"))
        Some("sonatype-snapshots" at nexus + "content/repositories/snapshots")
      else
        Some("sonatype-staging"  at nexus + "service/local/staging/deploy/maven2")
    },

    publishMavenStyle := true,
    useGpg in Global := true,
    publishArtifact in Test := false,
    pomIncludeRepository := { _ => false },
    pomExtra :=
      <url>http://shark.cs.berkeley.edu</url>
      <licenses>
        <license>
          <name>Apache 2.0</name>
          <url>http://www.apache.org/licenses/</url>
          <distribution>repo</distribution>
        </license>
      </licenses>
      <scm>
        <url>git@github.com:amplab/shark.git</url>
        <connection>scm:git:git@github.com:amplab/shark.git</connection>
      </scm>
      <developers>
        <developer>
          <id>marmbrus</id>
          <name>Michael Armbrust</name>
          <email>michael@databricks.com</email>
          <url>http://www.cs.berkeley.edu/~marmbrus</url>
        </developer>
        <developer>
          <id>rxin</id>
          <name>Reynold Xin</name>
          <email>reynoldx@gmail.com</email>
          <url>http://www.cs.berkeley.edu/~rxin</url>
          <organization>U.C. Berkeley Computer Science</organization>
          <organizationUrl>http://www.cs.berkeley.edu</organizationUrl>
        </developer>
      </developers>,

    testOptions in Test += Tests.Argument("-oF") // Full stack trace on test failures
  ) ++ org.scalastyle.sbt.ScalastylePlugin.Settings

  def assemblyProjSettings = Seq(
    jarName in assembly <<= version map { v => "shark-assembly-" + v + "-hadoop" + hadoopVersion + ".jar" }
  ) ++ assemblySettings ++ extraAssemblySettings

  def extraAssemblySettings() = Seq(
    test in assembly := {},
    excludedJars in assembly <<= (fullClasspath in assembly) map { cp =>
      // Ignore datanucleus jars.
      cp.filter { file => file.data.getName.contains("datanucleus") }
    },
    mergeStrategy in assembly := {
      case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
      case m if m.toLowerCase.matches("meta-inf.*\\.sf$") => MergeStrategy.discard
      case "META-INF/services/org.apache.hadoop.fs.FileSystem" => MergeStrategy.concat
      case "reference.conf" => MergeStrategy.concat
      case _ => MergeStrategy.first
    }
  )
}
