import sbt._

object Dependencies {

  private val AllTest = "test,it"

  private lazy val ScalaTestVersion = "3.0.5"
  private lazy val AkkaStreamVersion = "2.5.12"
  private lazy val KafkaClientsVersion = "1.0.1"
  private lazy val MockitoVersion = "2.18.0"
  private lazy val EmbeddedKafkaVersion = "1.1.0"
  private lazy val PureConfigVersion = "0.9.1"

  lazy val scalaTest = "org.scalatest" %% "scalatest" % ScalaTestVersion % AllTest
  lazy val akkaStream =
    List("com.typesafe.akka" %% "akka-stream" % AkkaStreamVersion,
         "com.typesafe.akka" %% "akka-stream-testkit" % AkkaStreamVersion % AllTest)
  lazy val kafka = "org.apache.kafka" % "kafka-clients" % KafkaClientsVersion
  lazy val mockito = "org.mockito" % "mockito-core" % MockitoVersion % AllTest
  lazy val embeddedKafka = "net.manub" %% "scalatest-embedded-kafka" % EmbeddedKafkaVersion % AllTest
  lazy val pureConfig = "com.github.pureconfig" %% "pureconfig" % PureConfigVersion
}
