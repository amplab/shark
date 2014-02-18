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
  val SHARK_VERSION = "0.9.0"

  val SHARK_ORGANIZATION = "edu.berkeley.cs.shark"

  val HIVE_VERSION = "0.11.0-shark"

  val SPARK_VERSION = "0.9.0-incubating"

  val SCALA_VERSION = "2.10.3"

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
  val excludeServlet = ExclusionRule(organization = "javax.servlet")
  val excludeXerces = ExclusionRule(organization = "xerces")

  // TODO(harvey): These should really be in a SharkHive project, but that requires re-organizing
  //               all of our settings. Should be done for v0.9.1. Also, we might not need some
  //               of these jars.
  val hiveArtifacts = Seq(
    "hive-anttasks",
    "hive-beeline",
    "hive-cli",
    "hive-common",
    "hive-exec",
    "hive-hbase-handler",
    "hive-hwi",
    "hive-jdbc",
    "hive-metastore",
    "hive-serde",
    "hive-service",
    "hive-shims")
  val hiveDependencies = hiveArtifacts.map ( artifactId =>
    SHARK_ORGANIZATION % artifactId % HIVE_VERSION excludeAll(
      excludeGuava, excludeLog4j, excludeServlet, excludeAsm, excludeNetty, excludeXerces)
  )

  val tachyonDependency = (if (TACHYON_ENABLED) {
    Some("org.tachyonproject" % "tachyon" % TACHYON_VERSION excludeAll(
      excludeKyro, excludeHadoop, excludeCurator, excludeJackson, excludeNetty, excludeAsm))
  } else {
    None
  }).toSeq

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
    scalacOptions := Seq("-deprecation", "-unchecked", "-optimize", "-feature", "-Yinline-warnings"),
    parallelExecution in Test := false,

    // Download managed jars into lib_managed.
    retrieveManaged := true,
    resolvers ++= Seq(
      "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
      "Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
      "Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/",
      "Sonatype Staging" at "https://oss.sonatype.org/service/local/staging/deploy/maven2/",
      "Local Maven" at Path.userHome.asFile.toURI.toURL + ".m2/repository"
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
    pomExtra := (
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
          <id>rxin</id>
          <name>Reynold Xin</name>
          <email>reynoldx@gmail.com</email>
          <url>http://www.cs.berkeley.edu/~rxin</url>
          <organization>U.C. Berkeley Computer Science</organization>
          <organizationUrl>http://www.cs.berkeley.edu</organizationUrl>
        </developer>
      </developers>
    ),

    fork := true,
    javaOptions += "-XX:MaxPermSize=512m",
    javaOptions += "-Xmx2g",
    javaOptions += "-Dsun.io.serialization.extendedDebugInfo=true",

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

    unmanagedJars in Test ++= Seq(
      file(System.getenv("HIVE_DEV_HOME")) / "build" / "ql" / "test" / "classes"
    ),
    libraryDependencies ++= hiveDependencies ++ tachyonDependency ++ yarnDependency,
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
      "org.apache.hadoop" % "hadoop-test" % "0.20.2" % "test" excludeAll(excludeJackson, excludeNetty, excludeAsm) force(),
      "org.scalatest" %% "scalatest" % "1.9.1" % "test",
      "junit" % "junit" % "4.10" % "test",
      "net.java.dev.jets3t" % "jets3t" % "0.7.1",
      "com.novocode" % "junit-interface" % "0.8" % "test")
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
