import sbt._
import Keys._
import sbtassembly.Plugin._
import AssemblyKeys._

object SharkBuild extends Build {

  // Hadoop version to build against. For example, "0.20.2", "0.20.205.0", or
  // "1.0.1" for Apache releases, or "0.20.2-cdh3u3" for Cloudera Hadoop.
  val HADOOP_VERSION = "0.20.205.0"

  // Spark version to build against.
  val SPARK_VERSION = "0.5.1-SNAPSHOT"

  lazy val root = Project(
    id = "root",
    base = file("."),
    settings = coreSettings)

  def coreSettings = Defaults.defaultSettings ++ Seq(

    name := "shark",
    organization := "edu.berkeley.cs.amplab",
    version := "0.1",
    scalaVersion := "2.9.1",
    scalacOptions := Seq("-deprecation", "-unchecked", "-optimize"),

    // Download managed jars into lib_managed.
    retrieveManaged := true,
    resolvers ++= Seq(
      "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
      "JBoss Repository" at "http://repository.jboss.org/nexus/content/repositories/releases/"
    ),

    testListeners <<= target.map(
      t => Seq(new eu.henkelmann.sbt.JUnitXmlTestsListener(t.getAbsolutePath))),

    unmanagedJars in Compile <++= baseDirectory map { base =>
      val hiveFile = file(System.getenv("HIVE_HOME")) / "lib"
      val baseDirectories = (base / "lib") +++ (hiveFile)
      val customJars = (baseDirectories ** "*.jar")
      // Hive uses an old version of guava that doesn't have what we want.
      customJars.classpath.filter(!_.toString.contains("guava"))
    },
    unmanagedJars in Test ++= Seq(
      file(System.getenv("HIVE_DEV_HOME")) / "build" / "ql" / "test" / "classes",
      file(System.getenv("HIVE_DEV_HOME")) / "build/ivy/lib/test/hadoop-test-0.20.2.jar"
    ),
    
    libraryDependencies ++= Seq(
      "org.spark-project" %% "spark-core" % SPARK_VERSION,
      "org.spark-project" %% "spark-repl" % SPARK_VERSION,
      "com.google.guava" % "guava" % "11.0.1",
      "org.apache.hadoop" % "hadoop-core" % HADOOP_VERSION,
      "it.unimi.dsi" % "fastutil" % "6.4.2",
      "org.scalatest" %% "scalatest" % "1.6.1" % "test",
      "junit" % "junit" % "4.10" % "test")

  ) ++ assemblySettings ++ Seq(test in assembly := {}) ++ Seq(getClassPathTask)

  // A sbt task to print out the classpath.
  val getClasspath = TaskKey[Unit]("getclasspath")
  val getClassPathTask = getClasspath <<= (target, fullClasspath in Runtime) map {
    (target, cp) => println(cp.map(_.data).mkString(":"))
  }

}

