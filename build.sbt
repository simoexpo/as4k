import Dependencies._

parallelExecution in Test := false

organization := "com.github.simoexpo"

scalaVersion := "2.12.5"

libraryDependencies ++= Seq(scalaTest, kafka, mockito, embeddedKafka) ++ akkaStream

scalacOptions in Compile := Seq("-deprecation")

lazy val as4k = (project in file("."))
    .configs(IntegrationTest)
    .settings(Defaults.itSettings)
