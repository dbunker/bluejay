name := "bluejay"

organization in ThisBuild := "com.bluejay"

scalaVersion in ThisBuild := "2.11.12"

lazy val global = project
  .in(file("."))
  .settings(settings)
  .aggregate(
    common,
    simple_count,
    word_count,
    nlp,
    vec_example,
    word_to_vec
  )

lazy val common = project
  .settings(
    name := "common",
    settings,
    libraryDependencies ++= commonDependencies
  )

lazy val simple_count = project
  .settings(
    name := "simple_count",
    settings,
    assemblySettings,
    libraryDependencies ++= commonDependencies
  )
  .dependsOn(
    common
  )

lazy val word_count = project
  .settings(
    name := "word_count",
    settings,
    assemblySettings,
    libraryDependencies ++= commonDependencies
  )
  .dependsOn(
    common
  )

lazy val nlp = project
  .settings(
    name := "nlp",
    settings,
    assemblySettings,
    libraryDependencies ++= commonDependencies ++ Seq(
      dependencies.nlpLib,
      dependencies.nlpModels
    )
  )
  .dependsOn(
    common
  )

lazy val vec_example = project
  .settings(
    name := "vec_example",
    settings,
    assemblySettings,
    libraryDependencies ++= commonDependencies ++ Seq(
      dependencies.sparkML
    )
  )
  .dependsOn(
    common
  )

lazy val word_to_vec = project
  .settings(
    name := "word_to_vec",
    settings,
    assemblySettings,
    libraryDependencies ++= commonDependencies ++ Seq(
      dependencies.sparkML
    )
  )
  .dependsOn(
    common
  )

lazy val dependencies =
  new {
    val spark     = "org.apache.spark" %% "spark-core"      % "2.2.1" % "provided"
    val sparkML   = "org.apache.spark" %% "spark-mllib"     % "2.2.1" % "provided"
    val jodaTime  = "joda-time"        % "joda-time"        % "2.9.9"
    val scalaTest = "org.scalatest"    % "scalatest_2.11"   % "3.0.4" % "test"
    val nlpLib    = "edu.stanford.nlp" % "stanford-corenlp" % "3.8.0"
    val nlpModels = "edu.stanford.nlp" % "stanford-corenlp" % "3.8.0" classifier "models"
  }

lazy val commonDependencies = Seq(
  dependencies.spark,
  dependencies.jodaTime,
  dependencies.scalaTest
)

lazy val settings =
commonSettings ++
wartremoverSettings ++
scalafmtSettings

lazy val compilerOptions = Seq(
  "-unchecked",
  "-feature",
  "-language:existentials",
  "-language:higherKinds",
  "-language:implicitConversions",
  "-language:postfixOps",
  "-deprecation",
  "-encoding",
  "utf8"
)

lazy val commonSettings = Seq(
  scalacOptions ++= compilerOptions,
  resolvers ++= Seq(
    "Local Maven Repository" at "file://" + Path.userHome.absolutePath + "/.m2/repository",
    Resolver.sonatypeRepo("releases"),
    Resolver.sonatypeRepo("snapshots")
  )
)

lazy val wartremoverSettings = Seq(
  wartremoverWarnings in (Compile, compile) ++= Warts.allBut(Wart.Throw)
)

lazy val scalafmtSettings =
  Seq(
    scalafmtOnCompile := true,
    scalafmtTestOnCompile := true,
    scalafmtVersion := "1.2.0"
  )

lazy val assemblySettings = Seq(
  assemblyJarName in assembly := name.value + ".jar",
  assemblyMergeStrategy in assembly := {
    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
    case _                             => MergeStrategy.first
  }
)
