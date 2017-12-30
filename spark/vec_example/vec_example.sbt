name := "vec_example"

version := "1.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "2.2.1" % "provided",
    "org.apache.spark" %% "spark-mllib" % "2.2.1" % "provided",
    "org.scalatest" % "scalatest_2.11" % "3.0.4" % "test"
)
