package org.simoexpo.as4k.it

import akka.stream.scaladsl.{Sink, Source}
import net.manub.embeddedkafka.EmbeddedKafka
import org.apache.kafka.common.serialization.StringSerializer
import org.scalatest.concurrent.ScalaFutures
import org.simoexpo.as4k.consumer.KafkaConsumerAgent
import org.simoexpo.as4k.it.testing.{ActorSystemSpec, BaseSpec, DataHelperSpec, LooseIntegrationPatience}
import org.simoexpo.as4k.model.{KRecord, KRecordMetadata}
import org.simoexpo.as4k.producer.{KafkaSimpleProducerAgent, KafkaTransactionalProducerAgent}
import org.simoexpo.as4k.{KSink, KSource}

import scala.util.Try

class KSinkIntegrationSpec
    extends BaseSpec
    with ActorSystemSpec
    with EmbeddedKafka
    with ScalaFutures
    with LooseIntegrationPatience
    with DataHelperSpec {

  implicit private val serializer: StringSerializer = new StringSerializer

  "KSink" should {

    val recordsSize = 100
    val kRecords = Range(0, recordsSize).map(n => aKRecord(n, n.toString, s"value$n", "input_topic", 1, "defaultGroup")).toList
    val messages = Range(0, recordsSize).map { n =>
      (n.toString, n.toString)
    }

    "allow to produce message individually with a simple producer" in {

      val outputTopic = getRandomTopicName

      Try(createCustomTopic(outputTopic))

      val kafkaConsumerOption = getKafkaSimpleConsumerOption(outputTopic)
      val kafkaConsumerAgent = new KafkaConsumerAgent(kafkaConsumerOption)

      val kafkaProducerOption = getKafkaSimpleProducerOption
      val kafkaProducerAgent = new KafkaSimpleProducerAgent(kafkaProducerOption)

      val consumedRecords = KSource.fromKafkaConsumer(kafkaConsumerAgent).take(recordsSize).runWith(Sink.seq)
      Source.fromIterator(() => kRecords.iterator).runWith(KSink.produce(outputTopic)(kafkaProducerAgent))

      whenReady(consumedRecords) { records =>
        records.map(record => (record.key, record.value)) shouldBe kRecords.map(record => (record.key, record.value))
      }
    }

    "allow to produce a sequence of message with a transactional producer" in {

      val outputTopic = getRandomTopicName

      Try(createCustomTopic(outputTopic))

      val kafkaConsumerOption = getKafkaSimpleConsumerOption(outputTopic)
      val kafkaConsumerAgent = new KafkaConsumerAgent(kafkaConsumerOption)

      val kafkaProducerOption = getKafkaTransactionalProducerOption
      val kafkaProducerAgent = new KafkaTransactionalProducerAgent(kafkaProducerOption)

      val consumedRecords = KSource.fromKafkaConsumer(kafkaConsumerAgent).take(recordsSize).runWith(Sink.seq)
      Source.fromIterator(() => kRecords.iterator).grouped(10).runWith(KSink.produceSequence(outputTopic)(kafkaProducerAgent))

      whenReady(consumedRecords) { records =>
        records.map(record => (record.key, record.value)) shouldBe kRecords.map(record => (record.key, record.value))
      }
    }

    "allow to produce and commit message individually in a transaction with a transactional producer" in {

      val inputTopic = getRandomTopicName
      val outputTopic = getRandomTopicName

      Try(createCustomTopic(inputTopic, partitions = 3))
      Try(createCustomTopic(outputTopic))
      publishToKafka(inputTopic, messages)

      val kafkaConsumerOption = getKafkaSimpleConsumerOption(inputTopic)
      val kafkaConsumerAgentOne = new KafkaConsumerAgent(kafkaConsumerOption, 3)
      val kafkaConsumerAgentTwo = new KafkaConsumerAgent(kafkaConsumerOption, 3)

      val kafkaProducerOption = getKafkaTransactionalProducerOption
      val kafkaProducerAgent = new KafkaTransactionalProducerAgent(kafkaProducerOption)

      val kafkaTransactionConsumerOption = getKafkaTransactionalConsumerOption(outputTopic)
      val kafkaTransactionConsumerAgent = new KafkaConsumerAgent(kafkaTransactionConsumerOption)

      val consumedRecords = KSource.fromKafkaConsumer(kafkaTransactionConsumerAgent).take(recordsSize).runWith(Sink.seq)

      for {
        _ <- KSource
          .fromKafkaConsumer(kafkaConsumerAgentOne)
          .take(recordsSize / 2)
          .runWith(KSink.produceAndCommit(outputTopic)(kafkaProducerAgent))
        _ <- kafkaConsumerAgentOne.stopConsumer
        _ <- KSource
          .fromKafkaConsumer(kafkaConsumerAgentTwo)
          .take(recordsSize / 2)
          .runWith(KSink.produceAndCommit(outputTopic)(kafkaProducerAgent))
      } yield ()

      whenReady(consumedRecords) { consumedMessages =>
        consumedMessages.map(record => (record.key, record.value)) should contain theSameElementsAs messages
      }
    }

    "allow to produce and commit a sequence of message in a transaction with a transactional producer" in {

      val inputTopic = getRandomTopicName
      val outputTopic = getRandomTopicName

      Try(createCustomTopic(inputTopic, partitions = 3))
      Try(createCustomTopic(outputTopic))
      publishToKafka(inputTopic, messages)

      val kafkaConsumerOption = getKafkaSimpleConsumerOption(inputTopic)
      val kafkaConsumerAgentOne = new KafkaConsumerAgent(kafkaConsumerOption, 3)
      val kafkaConsumerAgentTwo = new KafkaConsumerAgent(kafkaConsumerOption, 3)

      val kafkaProducerOption = getKafkaTransactionalProducerOption
      val kafkaProducerAgent = new KafkaTransactionalProducerAgent(kafkaProducerOption)

      val kafkaTransactionConsumerOption = getKafkaTransactionalConsumerOption(outputTopic)
      val kafkaTransactionConsumerAgent = new KafkaConsumerAgent(kafkaTransactionConsumerOption)

      val consumedRecords = KSource.fromKafkaConsumer(kafkaTransactionConsumerAgent).take(recordsSize).runWith(Sink.seq)

      for {
        _ <- KSource
          .fromKafkaConsumer(kafkaConsumerAgentOne)
          .take(recordsSize / 2)
          .grouped(10)
          .runWith(KSink.produceSequenceAndCommit(outputTopic)(kafkaProducerAgent))
        _ <- kafkaConsumerAgentOne.stopConsumer
        _ <- KSource
          .fromKafkaConsumer(kafkaConsumerAgentTwo)
          .take(recordsSize / 2)
          .grouped(10)
          .runWith(KSink.produceSequenceAndCommit(outputTopic)(kafkaProducerAgent))
      } yield ()

      whenReady(consumedRecords) { consumedMessages =>
        consumedMessages.map(record => (record.key, record.value)) should contain theSameElementsAs messages
      }
    }

    "allow to produce and commit a sequence of message in a transaction with a transactional producer in a multi-partition topic" in {

      val inputTopic = getRandomTopicName
      val outputTopic = getRandomTopicName

      Try(createCustomTopic(topic = inputTopic, partitions = 3))
      Try(createCustomTopic(outputTopic))
      publishToKafka(inputTopic, messages)

      val kafkaConsumerOption = getKafkaSimpleConsumerOption(inputTopic)
      val kafkaConsumerAgentOne = new KafkaConsumerAgent(kafkaConsumerOption, 3)
      val kafkaConsumerAgentTwo = new KafkaConsumerAgent(kafkaConsumerOption, 3)

      val kafkaProducerOption = getKafkaTransactionalProducerOption
      val kafkaProducerAgent = new KafkaTransactionalProducerAgent(kafkaProducerOption)

      val kafkaTransactionConsumerOption = getKafkaTransactionalConsumerOption(outputTopic)
      val kafkaTransactionConsumerAgent = new KafkaConsumerAgent(kafkaTransactionConsumerOption)

      val consumedRecords = KSource.fromKafkaConsumer(kafkaTransactionConsumerAgent).take(recordsSize).runWith(Sink.seq)

      for {
        _ <- KSource
          .fromKafkaConsumer(kafkaConsumerAgentOne)
          .take(recordsSize / 2)
          .grouped(10)
          .runWith(KSink.produceSequenceAndCommit(outputTopic)(kafkaProducerAgent))
        _ <- kafkaConsumerAgentOne.stopConsumer
        _ <- KSource
          .fromKafkaConsumer(kafkaConsumerAgentTwo)
          .take(recordsSize / 2)
          .grouped(10)
          .runWith(KSink.produceSequenceAndCommit(outputTopic)(kafkaProducerAgent))
      } yield ()

      whenReady(consumedRecords) { consumedMessages =>
        consumedMessages.map(record => (record.key, record.value)) should contain theSameElementsAs messages
      }
    }
  }

  private def aKRecord[K, V](offset: Long, key: K, value: V, topic: String, partition: Int, consumedBy: String): KRecord[K, V] = {
    val metadata = KRecordMetadata(topic, partition, offset, System.currentTimeMillis(), consumedBy)
    KRecord(key, value, metadata)
  }

}
