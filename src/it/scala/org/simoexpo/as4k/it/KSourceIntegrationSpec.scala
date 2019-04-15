package org.simoexpo.as4k.it

import akka.stream.scaladsl.Sink
import net.manub.embeddedkafka.EmbeddedKafka
import org.apache.kafka.common.serialization.StringSerializer
import org.scalatest.concurrent.ScalaFutures
import org.simoexpo.as4k.KSource
import org.simoexpo.as4k.consumer.KafkaConsumerAgent
import org.simoexpo.as4k.it.testing.{ActorSystemSpec, BaseSpec, DataHelperSpec, LooseIntegrationPatience}

import scala.util.Try

class KSourceIntegrationSpec
    extends BaseSpec
    with ActorSystemSpec
    with EmbeddedKafka
    with ScalaFutures
    with LooseIntegrationPatience
    with DataHelperSpec {

  implicit private val serializer: StringSerializer = new StringSerializer

  "KSource" should {

    val recordsSize = 100
    val messages = (0 until recordsSize).map { n =>
      (n.toString, n.toString)
    }

    "allow to consume message from a topic" in {

      val inputTopic = getRandomTopicName

      Try(createCustomTopic(topic = inputTopic, partitions = 3))
      publishToKafka(inputTopic, messages)

      val kafkaConsumerOption = getKafkaSimpleConsumerOption(inputTopic)
      val kafkaConsumerAgent = new KafkaConsumerAgent(kafkaConsumerOption)

      val result = KSource.fromKafkaConsumer(kafkaConsumerAgent).take(recordsSize).runWith(Sink.seq)

      whenReady(result) { consumedMessages =>
        consumedMessages.map(record => (record.key, record.value)) should contain theSameElementsAs messages
      }

    }

    "allow to consume message from a topic with multiple consumer" in {

      val inputTopic = getRandomTopicName

      Try(createCustomTopic(topic = inputTopic, partitions = 3))
      publishToKafka(inputTopic, messages)

      val kafkaConsumerOption = getKafkaSimpleConsumerOption(inputTopic)
      val kafkaConsumerAgent = new KafkaConsumerAgent(kafkaConsumerOption, 3)

      val result = KSource.fromKafkaConsumer(kafkaConsumerAgent).take(recordsSize).runWith(Sink.seq)

      whenReady(result) { consumedMessages =>
        consumedMessages.map(record => (record.key, record.value)) should contain theSameElementsAs messages
      }
    }
  }
}
