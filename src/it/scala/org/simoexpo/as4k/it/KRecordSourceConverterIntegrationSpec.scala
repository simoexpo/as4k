package org.simoexpo.as4k.it

import akka.stream.scaladsl.Sink
import net.manub.embeddedkafka.EmbeddedKafka
import org.apache.kafka.common.serialization.StringSerializer
import org.scalatest.concurrent.ScalaFutures
import org.simoexpo.as4k.KSource
import org.simoexpo.as4k.KSource._
import org.simoexpo.as4k.consumer.KafkaConsumerAgent
import org.simoexpo.as4k.it.testing.{ActorSystemSpec, BaseSpec, DataHelperSpec, LooseIntegrationPatience}
import org.simoexpo.as4k.producer.{KafkaSimpleProducerAgent, KafkaTransactionalProducerAgent}

import scala.util.Try

class KRecordSourceConverterIntegrationSpec
    extends BaseSpec
    with ActorSystemSpec
    with EmbeddedKafka
    with ScalaFutures
    with LooseIntegrationPatience
    with DataHelperSpec {

  implicit private val serializer: StringSerializer = new StringSerializer

  "KRecordSourceConverter" should {

    val recordsSize = 100
    val messages = Range(0, recordsSize).map { n =>
      (n.toString, n.toString)
    }

    "allow to map value of messages" in {

      val inputTopic = getRandomTopicName

      val mapFunction: String => String = (s: String) => s + "mapped"
      val mappedValueMessages = messages.map {
        case (key, value) => (key, mapFunction(value))
      }

      Try(createCustomTopic(inputTopic))
      publishToKafka(inputTopic, messages)

      val kafkaConsumerOption = getKafkaSimpleConsumerOption(inputTopic)
      val kafkaConsumerAgent = new KafkaConsumerAgent(kafkaConsumerOption)

      val result = KSource.fromKafkaConsumer(kafkaConsumerAgent).take(recordsSize).mapValue(mapFunction).runWith(Sink.seq)

      whenReady(result) { consumedMessages =>
        consumedMessages.map(record => (record.key, record.value)) shouldBe mappedValueMessages
      }
    }

    "allow to commit messages individually" in {

      val inputTopic = getRandomTopicName

      Try(createCustomTopic(inputTopic))
      publishToKafka(inputTopic, messages)

      val kafkaConsumerOption = getKafkaSimpleConsumerOption(inputTopic)
      val kafkaConsumerAgentOne = new KafkaConsumerAgent(kafkaConsumerOption)
      val kafkaConsumerAgentTwo = new KafkaConsumerAgent(kafkaConsumerOption)

      val resultFuture = for {
        firstResult <- KSource
          .fromKafkaConsumer(kafkaConsumerAgentOne)
          .take(recordsSize / 2)
          .commit()(kafkaConsumerAgentOne)
          .runWith(Sink.seq)
        _ <- kafkaConsumerAgentOne.stopConsumer
        secondResult <- KSource
          .fromKafkaConsumer(kafkaConsumerAgentTwo)
          .take(recordsSize / 2)
          .commit()(kafkaConsumerAgentTwo)
          .runWith(Sink.seq)
      } yield (firstResult, secondResult)

      whenReady(resultFuture) {
        case (firstHalf, secondHalf) =>
          (firstHalf ++ secondHalf).map(record => (record.key, record.value)) shouldBe messages
      }
    }

    "allow to commit messages individually in a multi-partitions topic" in {

      val inputTopic = getRandomTopicName

      Try(createCustomTopic(topic = inputTopic, partitions = 3))
      publishToKafka(inputTopic, messages)

      val kafkaConsumerOption = getKafkaSimpleConsumerOption(inputTopic)
      val kafkaConsumerAgentOne = new KafkaConsumerAgent(kafkaConsumerOption, 3)
      val kafkaConsumerAgentTwo = new KafkaConsumerAgent(kafkaConsumerOption, 3)

      val resultFuture = for {
        firstResult <- KSource
          .fromKafkaConsumer(kafkaConsumerAgentOne)
          .take(recordsSize / 2)
          .grouped(10)
          .commit(3)(kafkaConsumerAgentOne)
          .runWith(Sink.seq)
        _ <- kafkaConsumerAgentOne.stopConsumer
        secondResult <- KSource
          .fromKafkaConsumer(kafkaConsumerAgentTwo)
          .take(recordsSize / 2)
          .grouped(10)
          .commit(3)(kafkaConsumerAgentTwo)
          .runWith(Sink.seq)
      } yield (firstResult, secondResult)

      whenReady(resultFuture) {
        case (firstHalf, secondHalf) =>
          val consumedMessages = (firstHalf.flatten ++ secondHalf.flatten).map(record => (record.key, record.value))
          consumedMessages should contain theSameElementsAs messages
      }
    }

    "allow to produce message individually with a simple producer" in {

      val inputTopic = getRandomTopicName
      val outputTopic = getRandomTopicName

      Try(createCustomTopic(inputTopic))
      Try(createCustomTopic(outputTopic))
      publishToKafka(inputTopic, messages)

      val kafkaConsumerOption = getKafkaSimpleConsumerOption(inputTopic)
      val kafkaConsumerAgentOne = new KafkaConsumerAgent(kafkaConsumerOption)

      val kafkaProducerOption = getKafkaSimpleProducerOption
      val kafkaProducerAgent = new KafkaSimpleProducerAgent(kafkaProducerOption)

      val kafkaConsumerOptionTwo = getKafkaSimpleConsumerOption(inputTopic)
      val kafkaConsumerAgentTwo = new KafkaConsumerAgent(kafkaConsumerOptionTwo)

      KSource
        .fromKafkaConsumer(kafkaConsumerAgentOne)
        .take(recordsSize)
        .produce(3)(outputTopic)(kafkaProducerAgent)
        .runWith(Sink.ignore)

      val resultFuture = KSource.fromKafkaConsumer(kafkaConsumerAgentTwo).take(recordsSize).runWith(Sink.seq)

      whenReady(resultFuture) { consumedMessages =>
        consumedMessages.map(record => (record.key, record.value)) shouldBe messages
      }
    }

    "allow to produce and commit message individually in a transaction with a transactional producer" in {

      val inputTopic = getRandomTopicName
      val outputTopic = getRandomTopicName

      Try(createCustomTopic(inputTopic))
      Try(createCustomTopic(outputTopic))
      publishToKafka(inputTopic, messages)

      val kafkaConsumerOption = getKafkaSimpleConsumerOption(inputTopic)
      val kafkaConsumerAgentOne = new KafkaConsumerAgent(kafkaConsumerOption)
      val kafkaConsumerAgentTwo = new KafkaConsumerAgent(kafkaConsumerOption)

      val kafkaProducerOption = getKafkaTransactionalProducerOption
      val kafkaProducerAgent = new KafkaTransactionalProducerAgent(kafkaProducerOption)

      val kafkaTransactionConsumerOption = getKafkaTransactionalConsumerOption(outputTopic)
      val kafkaTransactionConsumerAgent = new KafkaConsumerAgent(kafkaTransactionConsumerOption)

      val consumedRecords = KSource.fromKafkaConsumer(kafkaTransactionConsumerAgent).take(recordsSize).runWith(Sink.seq)

      for {
        _ <- KSource
          .fromKafkaConsumer(kafkaConsumerAgentOne)
          .take(recordsSize / 2)
          .produceAndCommit(outputTopic)(kafkaProducerAgent)
          .runWith(Sink.ignore)
        _ <- kafkaConsumerAgentOne.stopConsumer
        _ <- KSource
          .fromKafkaConsumer(kafkaConsumerAgentTwo)
          .take(recordsSize / 2)
          .produceAndCommit(outputTopic)(kafkaProducerAgent)
          .runWith(Sink.ignore)
      } yield ()

      whenReady(consumedRecords) { consumedMessages =>
        consumedMessages.map(record => (record.key, record.value)) shouldBe messages
      }
    }
  }
}
