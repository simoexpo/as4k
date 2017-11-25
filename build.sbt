import Dependencies._

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "com.github.simoexpo",
      scalaVersion := "2.12.4",
      version      := "0.1.0-SNAPSHOT"
    )),
    name := "as4k",
    libraryDependencies += scalaTest,
    libraryDependencies ++= akkaStream,
    libraryDependencies += kafka,
    libraryDependencies += mockito

)
