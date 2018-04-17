package org.simoexpo.as4k.benchmark

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, NewTopic}

import scala.concurrent.{ExecutionContext, Future}
import scala.collection.JavaConverters._
import scala.util.control.NonFatal

trait KafkaManagerUtility {

  private lazy val config = ConfigFactory.load()

  private val kafkaBootstrapServers = config.getString("kafka-bootstrap-servers")

  private val props = new Properties()
  props.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers)

  private val adminClient = AdminClient.create(props)

  def createSimpleTopics(topics: Seq[String])(implicit ec: ExecutionContext): Future[Unit] = {

    val newTopics = topics.map(topicName => new NewTopic(topicName, 1, 1)).asJava

    for {
      _ <- Future(adminClient.deleteTopics(topics.asJava).all().get()).recoverWith {
        case NonFatal(_) => Future.unit
      }
      _ <- Future(adminClient.createTopics(newTopics).all().get())
    } yield ()

  }

}
