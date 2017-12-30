name := "nlp"

version := "1.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "2.2.1" % "provided",
    "edu.stanford.nlp" % "stanford-corenlp" % "3.8.0",
    "edu.stanford.nlp" % "stanford-corenlp" % "3.8.0" classifier "models",
    "org.scalatest" % "scalatest_2.11" % "3.0.4" % "test"
)
