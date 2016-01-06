name := "vec_example"

version := "1.0"

scalaVersion := "2.10.6"

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "1.5.2" % "provided",
    "org.apache.spark" %% "spark-mllib" % "1.5.2" % "provided"
)

mergeStrategy in assembly <<= (mergeStrategy in assembly) { mergeStrategy => {
    case entry => {
        val strategy = mergeStrategy(entry)
            if (strategy == MergeStrategy.deduplicate) MergeStrategy.first
            else strategy
        }
    }
}