import sbt._

object Dependencies {

  private val AllTest = "test,it"

  lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.0.5" % AllTest
  lazy val akkaStream =
    List("com.typesafe.akka" %% "akka-stream" % "2.5.11", "com.typesafe.akka" %% "akka-stream-testkit" % "2.5.11" % AllTest)
  lazy val kafka = "org.apache.kafka" % "kafka-clients" % "1.0.1"
  lazy val mockito = "org.mockito" % "mockito-core" % "2.16.0" % AllTest
  lazy val embeddedKafka = "net.manub" %% "scalatest-embedded-kafka" % "1.1.0" % AllTest
}
