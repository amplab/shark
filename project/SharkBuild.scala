import sbt._
import Keys._
import sbtassembly.Plugin._
import AssemblyKeys._

object SharkBuild extends Build {

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
      "JBoss Repository" at "http://repository.jboss.org/nexus/content/repositories/releases/",
      "Twitter Repository" at "http://maven.twttr.com/"
    ),

    unmanagedJars in Compile <++= baseDirectory map { base =>
      val hiveFile = file(System.getenv("HIVE_HOME")) / "lib"
      val baseDirectories = (base / "lib") +++ (hiveFile)
      val customJars = (baseDirectories ** "*.jar")
      // Hive uses an old version of guava that doesn't have what we want.
      customJars.classpath.filter(!_.toString.contains("guava"))
    },
    unmanagedJars in Test ++= Seq(
      file(System.getenv("HIVE_DEV_HOME")) / "build" / "ql" / "test" / "classes",
      file(System.getenv("HIVE_DEV_HOME")) / "build/hadoopcore/hadoop-0.20.1/hadoop-0.20.1-test.jar"
    ),

    libraryDependencies ++= Seq(
      "org.spark-project" %% "spark-core" % "0.4-SNAPSHOT",
      "org.spark-project" %% "spark-repl" % "0.4-SNAPSHOT",
      "com.twitter" %% "json" % "2.1.7",
      "com.google.guava" % "guava" % "11.0.1",
      "org.apache.hadoop" % "hadoop-core" % "0.20.2",
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

