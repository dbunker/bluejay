name := "simple_count"

version := "1.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "2.2.1" % "provided",
    "joda-time" % "joda-time" % "2.9.9",
    "org.scalatest" % "scalatest_2.11" % "3.0.4" % "test"
)
