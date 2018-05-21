val globalSettings = Seq(
  organization := "com.monovore.hunger",
  version := "0.1-SNAPSHOT",
  scalaVersion := "2.11.11",
  scalacOptions := Seq("-Ypartial-unification", "-Xexperimental", "-Xmax-classfile-name", "100"),
  javacOptions ++= Seq("-source", "1.8", "-target", "1.8", "-Xlint:unchecked"),
  cancelable in Global := true,
  fork := true
)

lazy val root =
  project.in(file("."))
    .aggregate(core)
    .settings(globalSettings: _*)

lazy val core =
  project.in(file("core"))
    .settings(globalSettings: _*)
