package org.simoexpo.as4k.it

import akka.stream.scaladsl.{Sink, Source}
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.common.serialization.StringSerializer
import org.scalatest.concurrent.ScalaFutures
import org.simoexpo.as4k.consumer.{KafkaConsumerAgent, KafkaConsumerOption}
import org.simoexpo.as4k.it.testing.{ActorSystemSpec, BaseSpec, LooseIntegrationPatience}
import org.simoexpo.as4k.model.KRecord
import org.simoexpo.as4k.producer.{KafkaProducerOption, KafkaSimpleProducerAgent, KafkaTransactionalProducerAgent}
import org.simoexpo.as4k.{KSink, KSource}

class KSinkIntegrationSpec
    extends BaseSpec
    with ActorSystemSpec
    with EmbeddedKafka
    with ScalaFutures
    with LooseIntegrationPatience {

  "KSink" should {

    val inputTopic = "in_topic"
    val outputTopic = "out_topic"

    implicit val config = EmbeddedKafkaConfig(9092, 2181)

    val kafkaConsumerOption: KafkaConsumerOption[String, String] = KafkaConsumerOption(Seq(outputTopic), "my-consumer")

    val kafkaSimpleProducerOption: KafkaProducerOption[String, String] = KafkaProducerOption(outputTopic, "my-simple-producer")

    val kafkaTransactionalProducerOption: KafkaProducerOption[String, String] =
      KafkaProducerOption(outputTopic, "my-transactional-producer")

    val kRecords = Range(0, 100).map(n => aKRecord(n, n.toString, s"value$n", inputTopic, 1)).toList

    "allow to produce message individually with a simple producer" in {

      withRunningKafka {
        createCustomTopic(outputTopic)

        implicit val serializer: StringSerializer = new StringSerializer

        val kafkaConsumerAgent = new KafkaConsumerAgent(kafkaConsumerOption, 100)

        val kafkaProducerAgent = new KafkaSimpleProducerAgent(kafkaSimpleProducerOption)

        val consumedRecords = KSource.fromKafkaConsumer(kafkaConsumerAgent).take(100).runWith(Sink.seq)
        Source.fromIterator(() => kRecords.iterator).runWith(KSink.produce(kafkaProducerAgent))

        whenReady(consumedRecords) { records =>
          records.map(record => (record.key, record.value)) shouldBe kRecords.map(record => (record.key, record.value))
        }
      }

    }

    "allow to produce a sequence of message with a transactional producer" in {

      withRunningKafka {
        createCustomTopic(outputTopic)

        implicit val serializer: StringSerializer = new StringSerializer

        val kafkaConsumerAgent = new KafkaConsumerAgent(kafkaConsumerOption, 100)

        val kafkaProducerAgent = new KafkaTransactionalProducerAgent(kafkaTransactionalProducerOption)

        val consumedRecords = KSource.fromKafkaConsumer(kafkaConsumerAgent).take(100).runWith(Sink.seq)
        Source.fromIterator(() => kRecords.iterator).grouped(10).runWith(KSink.produceSequence(kafkaProducerAgent))

        whenReady(consumedRecords) { records =>
          records.map(record => (record.key, record.value)) shouldBe kRecords.map(record => (record.key, record.value))
        }
      }

    }

    "allow to produce and commit message individually in a transaction with a transactional producer" in {

      val messages = Range(0, 100).map { n =>
        (n.toString, n.toString)
      }

      withRunningKafka {
        createCustomTopic(inputTopic)

        createCustomTopic(outputTopic)

        implicit val serializer: StringSerializer = new StringSerializer

        publishToKafka(inputTopic, messages)

        val kafkaConsumerAgentOne = new KafkaConsumerAgent(kafkaConsumerOption.copy(topics = Seq(inputTopic)), 100)

        val kafkaConsumerAgentTwo = new KafkaConsumerAgent(kafkaConsumerOption.copy(topics = Seq(inputTopic)), 100)

        val kafkaProducerAgent = new KafkaTransactionalProducerAgent(kafkaTransactionalProducerOption)

        val kafkaConsumerOptionThree: KafkaConsumerOption[String, String] = KafkaConsumerOption(Seq(outputTopic), "my-consumer-2")

        val kafkaConsumerAgentThree = new KafkaConsumerAgent(kafkaConsumerOptionThree, 100)

        val consumedRecords = KSource.fromKafkaConsumer(kafkaConsumerAgentThree).take(100).runWith(Sink.seq)

        for {
          _ <- KSource
            .fromKafkaConsumer(kafkaConsumerAgentOne)
            .take(50)
            .runWith(KSink.produceAndCommit(kafkaProducerAgent, kafkaConsumerAgentOne))
          _ <- kafkaConsumerAgentOne.stopConsumer
          _ <- KSource
            .fromKafkaConsumer(kafkaConsumerAgentTwo)
            .take(50)
            .runWith(KSink.produceAndCommit(kafkaProducerAgent, kafkaConsumerAgentTwo))
        } yield ()

        whenReady(consumedRecords) { consumedMessages =>
          consumedMessages.map(record => (record.key, record.value)) shouldBe messages
        }
      }
    }

    "allow to produce and commit a sequence of message in a transaction with a transactional producer" in {

      val messages = Range(0, 100).map { n =>
        (n.toString, n.toString)
      }

      withRunningKafka {
        createCustomTopic(inputTopic)

        createCustomTopic(outputTopic)

        implicit val serializer: StringSerializer = new StringSerializer

        publishToKafka(inputTopic, messages)

        val kafkaConsumerAgentOne = new KafkaConsumerAgent(kafkaConsumerOption.copy(topics = Seq(inputTopic)), 100)

        val kafkaConsumerAgentTwo = new KafkaConsumerAgent(kafkaConsumerOption.copy(topics = Seq(inputTopic)), 100)

        val kafkaProducerAgent = new KafkaTransactionalProducerAgent(kafkaTransactionalProducerOption)

        val kafkaConsumerOptionThree: KafkaConsumerOption[String, String] = KafkaConsumerOption(Seq(outputTopic), "my-consumer-2")

        val kafkaConsumerAgentThree = new KafkaConsumerAgent(kafkaConsumerOptionThree, 100)

        val consumedRecords = KSource.fromKafkaConsumer(kafkaConsumerAgentThree).take(100).runWith(Sink.seq)

        for {
          _ <- KSource
            .fromKafkaConsumer(kafkaConsumerAgentOne)
            .take(50)
            .grouped(10)
            .runWith(KSink.produceSequenceAndCommit(kafkaProducerAgent, kafkaConsumerAgentOne))
          _ <- kafkaConsumerAgentOne.stopConsumer
          _ <- KSource
            .fromKafkaConsumer(kafkaConsumerAgentTwo)
            .take(50)
            .grouped(10)
            .runWith(KSink.produceSequenceAndCommit(kafkaProducerAgent, kafkaConsumerAgentTwo))
        } yield ()

        whenReady(consumedRecords) { consumedMessages =>
          consumedMessages.map(record => (record.key, record.value)) shouldBe messages
        }
      }

    }
  }

  protected def aKRecord[K, V](offset: Long, key: K, value: V, topic: String, partition: Int) =
    KRecord(key, value, topic, partition, offset, System.currentTimeMillis())
}
