organization := "com.monovore.hunger"

name := "hunger-artist"

version := "0.1-SNAPSHOT"

libraryDependencies ++= Seq(
  "com.google.guava" % "guava" % "19.0",
  "org.apache.kafka" % "kafka-clients" % "0.9.0.1",
  "ch.qos.logback" % "logback-classic" % "1.1.6" % "test"
)

scalaVersion := "2.11.7"

scalacOptions := Seq("-Xexperimental")

cancelable in Global := true

fork in run := true