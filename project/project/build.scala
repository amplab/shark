import sbt._

object Plugins extends Build {
  lazy val root = Project("root", file(".")) dependsOn(
    uri("git://github.com/sbt/sbt-assembly.git#071"),
    uri("git://github.com/ijuma/junit_xml_listener.git#fe434773255b451a38e8d889536ebc260f4225ce")
  )
}


