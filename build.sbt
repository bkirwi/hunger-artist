organization := "com.monovore.hunger"

name := "hunger-artist"

version := "0.1-SNAPSHOT"

libraryDependencies ++= Seq(
  "org.typelevel" %% "cats-core" % "1.0.1",
  "org.typelevel" %% "cats-effect" % "0.8",
  "co.fs2" %% "fs2-core" % "0.10.0-RC1",
  "org.apache.kafka" % "kafka-clients" % "1.0.0",
  "ch.qos.logback" % "logback-classic" % "1.1.6" % "test",
  "net.manub" %% "scalatest-embedded-kafka" % "1.0.0" % "test"
)

scalaVersion := "2.11.11"

scalacOptions := Seq("-Ypartial-unification", "-Xexperimental", "-Xmax-classfile-name", "100")

javacOptions ++= Seq("-source", "1.8", "-target", "1.8", "-Xlint:unchecked")

cancelable in Global := true

fork := true
