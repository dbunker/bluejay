name := "nlp"

version := "1.0"

scalaVersion := "2.10.6"

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "1.5.0" % "provided",
    "edu.stanford.nlp" % "stanford-corenlp" % "3.5.1",
    "joda-time" % "joda-time" % "2.8.2"
)

mergeStrategy in assembly <<= (mergeStrategy in assembly) { mergeStrategy => {
    case entry => {
        val strategy = mergeStrategy(entry)
            if (strategy == MergeStrategy.deduplicate) MergeStrategy.first
            else strategy
        }
    }
}