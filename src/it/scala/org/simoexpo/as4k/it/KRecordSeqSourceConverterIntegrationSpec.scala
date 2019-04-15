package org.simoexpo.as4k.it

import akka.stream.scaladsl.Sink
import net.manub.embeddedkafka.EmbeddedKafka
import org.apache.kafka.common.serialization.StringSerializer
import org.scalatest.concurrent.ScalaFutures
import org.simoexpo.as4k.KSource
import org.simoexpo.as4k.KSource._
import org.simoexpo.as4k.consumer.KafkaConsumerAgent
import org.simoexpo.as4k.it.testing.{ActorSystemSpec, BaseSpec, DataHelperSpec, LooseIntegrationPatience}
import org.simoexpo.as4k.producer.KafkaTransactionalProducerAgent

import scala.util.Try

class KRecordSeqSourceConverterIntegrationSpec
    extends BaseSpec
    with ActorSystemSpec
    with EmbeddedKafka
    with ScalaFutures
    with LooseIntegrationPatience
    with DataHelperSpec {

  implicit private val serializer: StringSerializer = new StringSerializer

  "KRecordSeqSourceConverter" should {

    val recordsSize = 100
    val messages = Range(0, recordsSize).map { n =>
      (n.toString, n.toString)
    }

    "allow to commit a sequence of message" in {

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
          (firstHalf ++ secondHalf).flatten.map(record => (record.key, record.value)) shouldBe messages
      }
    }

    "allow to commit a sequence of message in a multi-partitions topic" in {

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

    "allow to produce a sequence of message with a transactional producer" in {

      val inputTopic = getRandomTopicName
      val outputTopic = getRandomTopicName

      Try(createCustomTopic(inputTopic))
      Try(createCustomTopic(outputTopic))
      publishToKafka(inputTopic, messages)

      val kafkaConsumerOption = getKafkaSimpleConsumerOption(inputTopic)
      val kafkaConsumerAgentOne = new KafkaConsumerAgent(kafkaConsumerOption)

      val kafkaTransactionalProducerOption = getKafkaTransactionalProducerOption
      val kafkaProducerAgent = new KafkaTransactionalProducerAgent(kafkaTransactionalProducerOption)

      val kafkaTransactionConsumerOption = getKafkaTransactionalConsumerOption(outputTopic)
      val kafkaTransactionConsumerAgent = new KafkaConsumerAgent(kafkaTransactionConsumerOption)

      KSource
        .fromKafkaConsumer(kafkaConsumerAgentOne)
        .take(recordsSize)
        .grouped(10)
        .produce(outputTopic)(kafkaProducerAgent)
        .runWith(Sink.ignore)
      val resultFuture = KSource.fromKafkaConsumer(kafkaTransactionConsumerAgent).take(recordsSize).runWith(Sink.seq)

      whenReady(resultFuture) { consumedMessages =>
        consumedMessages.map(record => (record.key, record.value)) shouldBe messages
      }
    }

    "allow to produce and commit a sequence of message in a transaction with a transactional producer" in {

      val inputTopic = getRandomTopicName
      val outputTopic = getRandomTopicName

      Try(createCustomTopic(inputTopic))
      Try(createCustomTopic(outputTopic))
      publishToKafka(inputTopic, messages)

      val kafkaConsumerOption = getKafkaSimpleConsumerOption(inputTopic)
      val kafkaConsumerAgentOne = new KafkaConsumerAgent(kafkaConsumerOption)
      val kafkaConsumerAgentTwo = new KafkaConsumerAgent(kafkaConsumerOption)

      val kafkaTransactionalProducerOption = getKafkaTransactionalProducerOption
      val kafkaProducerAgent = new KafkaTransactionalProducerAgent(kafkaTransactionalProducerOption)

      val kafkaTransactionConsumerOption = getKafkaTransactionalConsumerOption(outputTopic)
      val kafkaTransactionConsumerAgent = new KafkaConsumerAgent(kafkaTransactionConsumerOption)

      val consumedRecords = KSource.fromKafkaConsumer(kafkaTransactionConsumerAgent).take(recordsSize).runWith(Sink.seq)

      for {
        _ <- KSource
          .fromKafkaConsumer(kafkaConsumerAgentOne)
          .take(recordsSize / 2)
          .grouped(10)
          .produceAndCommit(outputTopic)(kafkaProducerAgent)
          .runWith(Sink.ignore)
        _ <- kafkaConsumerAgentOne.stopConsumer
        _ <- KSource
          .fromKafkaConsumer(kafkaConsumerAgentTwo)
          .take(recordsSize / 2)
          .grouped(10)
          .produceAndCommit(outputTopic)(kafkaProducerAgent)
          .runWith(Sink.ignore)
      } yield ()

      whenReady(consumedRecords) { consumedMessages =>
        consumedMessages.map(record => (record.key, record.value)) shouldBe messages
      }
    }

    "allow to produce and commit a sequence of message in a transaction with a transactional producer in a multi-partition topic" in {

      val inputTopic = getRandomTopicName
      val outputTopic = getRandomTopicName

      Try(createCustomTopic(topic = inputTopic, partitions = 3))
      Try(createCustomTopic(outputTopic))
      publishToKafka(inputTopic, messages)

      val kafkaConsumerOption = getKafkaSimpleConsumerOption(inputTopic)
      val kafkaConsumerAgentOne = new KafkaConsumerAgent(kafkaConsumerOption)
      val kafkaConsumerAgentTwo = new KafkaConsumerAgent(kafkaConsumerOption)

      val kafkaTransactionalProducerOption = getKafkaTransactionalProducerOption
      val kafkaProducerAgent = new KafkaTransactionalProducerAgent(kafkaTransactionalProducerOption)

      val kafkaTransactionConsumerOption = getKafkaTransactionalConsumerOption(outputTopic)
      val kafkaTransactionConsumerAgent = new KafkaConsumerAgent(kafkaTransactionConsumerOption)

      val consumedRecords = KSource.fromKafkaConsumer(kafkaTransactionConsumerAgent).take(recordsSize).runWith(Sink.seq)

      for {
        _ <- KSource
          .fromKafkaConsumer(kafkaConsumerAgentOne)
          .take(recordsSize / 2)
          .grouped(10)
          .produceAndCommit(outputTopic)(kafkaProducerAgent)
          .runWith(Sink.ignore)
        _ <- kafkaConsumerAgentOne.stopConsumer
        _ <- KSource
          .fromKafkaConsumer(kafkaConsumerAgentTwo)
          .take(recordsSize / 2)
          .grouped(10)
          .produceAndCommit(outputTopic)(kafkaProducerAgent)
          .runWith(Sink.ignore)
      } yield ()

      whenReady(consumedRecords) { consumedMessages =>
        consumedMessages.map(record => (record.key, record.value)).toSet shouldBe messages.toSet
      }
    }
  }
}
