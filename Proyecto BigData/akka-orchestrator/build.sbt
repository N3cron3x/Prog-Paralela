name := "bigdata-orchestrator"
version := "1.0"
scalaVersion := "2.13.12"

val AkkaVersion = "2.8.5"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor-typed" % AkkaVersion,
  "com.typesafe.akka" %% "akka-stream" % AkkaVersion,
  "io.spray" %% "spray-json" % "1.3.6",
  "com.amazonaws" % "aws-lambda-java-core" % "1.2.3",
  "com.amazonaws" % "aws-lambda-java-events" % "3.11.4",
  "software.amazon.awssdk" % "lambda" % "2.25.60",
  "software.amazon.awssdk" % "s3" % "2.25.60",
  "com.typesafe.akka" %% "akka-actor-testkit-typed" % AkkaVersion % Test
)

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case _ => MergeStrategy.first
}
