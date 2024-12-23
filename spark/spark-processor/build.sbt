name := "EventProcessor"

version := "1.0"

scalaVersion := "2.12.18"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.5.4",
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % "3.5.4",
  "org.postgresql" % "postgresql" % "42.7.4",
)
