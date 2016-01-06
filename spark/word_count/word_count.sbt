name := "word_count"

version := "1.0"

scalaVersion := "2.10.6"

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "1.5.2" % "provided",
    "joda-time" % "joda-time" % "2.8.2",
    "org.scalatest" % "scalatest_2.10" % "2.2.4" % "test"
)

mergeStrategy in assembly <<= (mergeStrategy in assembly) { mergeStrategy => {
    case entry => {
        val strategy = mergeStrategy(entry)
            if (strategy == MergeStrategy.deduplicate) MergeStrategy.first
            else strategy
        }
    }
}