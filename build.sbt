import Dependencies._
import Configurations._

parallelExecution in Test := false
parallelExecution in IntegrationTest := false

organization := "com.github.simoexpo"

scalaVersion := "2.12.5"

libraryDependencies ++= Seq(scalaTest, kafka, mockito, embeddedKafka, pureConfig) ++ akkaStream

scalacOptions in Compile := Seq("-deprecation")

lazy val as4k = (project in file("."))
    .configs(IntegrationTest, Benchmark)
    .settings(Defaults.itSettings, benchmarkSettings)

addCommandAlias("bench", ";bench:startKafka;bench:run;bench:stopKafka")
