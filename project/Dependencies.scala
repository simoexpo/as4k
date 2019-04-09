import sbt._

object Dependencies {

  private val AllTest = "test,it"

  private lazy val ScalaTestVersion = "3.0.7"
  private lazy val AkkaStreamVersion = "2.5.22"
  private lazy val KafkaClientsVersion = "2.2.0"
  private lazy val MockitoVersion = "2.26.0"
  private lazy val EmbeddedKafkaVersion = "2.2.0"
  private lazy val PureConfigVersion = "0.10.2"

  lazy val scalaTest = "org.scalatest" %% "scalatest" % ScalaTestVersion % AllTest
  lazy val akkaStream =
    List("com.typesafe.akka" %% "akka-stream" % AkkaStreamVersion,
         "com.typesafe.akka" %% "akka-stream-testkit" % AkkaStreamVersion % AllTest)
  lazy val kafka = "org.apache.kafka" % "kafka-clients" % KafkaClientsVersion
  lazy val mockito = "org.mockito" % "mockito-core" % MockitoVersion % AllTest
  lazy val embeddedKafka = "io.github.embeddedkafka" %% "embedded-kafka" % EmbeddedKafkaVersion % AllTest
  lazy val pureConfig = "com.github.pureconfig" %% "pureconfig" % PureConfigVersion
}
