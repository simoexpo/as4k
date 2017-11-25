import sbt._

object Dependencies {
  lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.0.1" % Test
  lazy val akkaStream =
    List("com.typesafe.akka" %% "akka-stream" % "2.5.9", "com.typesafe.akka" %% "akka-stream-testkit" % "2.5.9" % Test)
  lazy val kafka = "org.apache.kafka" % "kafka-clients" % "1.0.0"
  lazy val mockito = "org.mockito" % "mockito-core" % "2.15.0" % Test
}
