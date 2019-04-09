package org.simoexpo.as4k.it

import akka.stream.scaladsl.Sink
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.common.serialization.StringSerializer
import org.scalatest.concurrent.ScalaFutures
import org.simoexpo.as4k.KSource
import org.simoexpo.as4k.KSource._
import org.simoexpo.as4k.consumer.{KafkaConsumerAgent, KafkaConsumerOption}
import org.simoexpo.as4k.it.testing.{ActorSystemSpec, BaseSpec, LooseIntegrationPatience}
import org.simoexpo.as4k.producer.{KafkaProducerOption, KafkaSimpleProducerAgent, KafkaTransactionalProducerAgent}

import scala.util.Try

class KSourceIntegrationSpec
    extends BaseSpec
    with ActorSystemSpec
    with EmbeddedKafka
    with ScalaFutures
    with LooseIntegrationPatience {

  "KSource" should {

    val inputTopic = "in_topic"
    val outputTopic = "out_topic"

    implicit val config: EmbeddedKafkaConfig = EmbeddedKafkaConfig(kafkaPort = 9092, zooKeeperPort = 2181)

    val kafkaConsumerOption: KafkaConsumerOption[String, String] = KafkaConsumerOption(Seq(inputTopic), "my-consumer")

    val kafkaSimpleProducerOption: KafkaProducerOption[String, String] = KafkaProducerOption(outputTopic, "my-simple-producer")

    val kafkaTransactionalProducerOption: KafkaProducerOption[String, String] =
      KafkaProducerOption(outputTopic, "my-transactional-producer")

    val recordsSize = 100

    "allow to consume message from a topic" in {

      val messages = Range(0, 100).map { n =>
        (n.toString, n.toString)
      }

      withRunningKafka {
        Try(createCustomTopic(inputTopic))

        implicit val serializer: StringSerializer = new StringSerializer

        publishToKafka(inputTopic, messages)

        val kafkaConsumerAgent = new KafkaConsumerAgent(kafkaConsumerOption)

        val result = KSource.fromKafkaConsumer(kafkaConsumerAgent).take(recordsSize).runWith(Sink.seq)

        whenReady(result) { consumedMessages =>
          consumedMessages.map(record => (record.key, record.value)) shouldBe messages
        }
      }

    }

    "allow to commit message individually" in {

      val messages = Range(0, recordsSize).map { n =>
        (n.toString, n.toString)
      }

      withRunningKafka {
        Try(createCustomTopic(inputTopic))

        implicit val serializer: StringSerializer = new StringSerializer

        publishToKafka(inputTopic, messages)

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
            firstHalf.map(record => (record.key, record.value)) shouldBe messages.take(recordsSize / 2)
            secondHalf.map(record => (record.key, record.value)) shouldBe messages.drop(recordsSize / 2)
        }
      }
    }

    "allow to commit a sequence of message" in {

      val messages = Range(0, recordsSize).map { n =>
        (n.toString, n.toString)
      }

      withRunningKafka {
        Try(createCustomTopic(inputTopic))

        implicit val serializer: StringSerializer = new StringSerializer

        publishToKafka(inputTopic, messages)

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
            firstHalf.flatMap(records => records.map(record => (record.key, record.value))) shouldBe messages.take(
              recordsSize / 2)
            secondHalf.flatMap(records => records.map(record => (record.key, record.value))) shouldBe messages.drop(
              recordsSize / 2)
        }
      }
    }

    "allow to commit a sequence of message in a multi-partition topic" in {

      val messages = Range(0, recordsSize).map { n =>
        (n.toString, n.toString)
      }

      withRunningKafka {
        Try(createCustomTopic(topic = inputTopic, partitions = 3))

        implicit val serializer: StringSerializer = new StringSerializer

        publishToKafka(inputTopic, messages)

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
            (firstHalf.flatten ++ secondHalf.flatten).map(record => (record.key, record.value)).toSet shouldBe messages.toSet
        }
      }
    }

    "allow to map value of messages" in {

      val messages = Range(0, recordsSize).map { n =>
        (n.toString, n.toString)
      }

      val mapFunction: String => String = (s: String) => s + "mapped"

      val mappedValueMessages = messages.map {
        case (key, value) => (key, mapFunction(value))
      }

      withRunningKafka {
        Try(createCustomTopic(inputTopic))

        implicit val serializer: StringSerializer = new StringSerializer

        publishToKafka(inputTopic, messages)

        val kafkaConsumerAgent = new KafkaConsumerAgent(kafkaConsumerOption)

        val result = KSource.fromKafkaConsumer(kafkaConsumerAgent).take(recordsSize).mapValue(mapFunction).runWith(Sink.seq)

        whenReady(result) { consumedMessages =>
          consumedMessages.map(record => (record.key, record.value)) shouldBe mappedValueMessages
        }
      }

    }

    "allow to produce message individually with a simple producer" in {

      val messages = Range(0, recordsSize).map { n =>
        (n.toString, n.toString)
      }

      withRunningKafka {
        Try(createCustomTopic(inputTopic))

        Try(createCustomTopic(outputTopic))

        implicit val serializer: StringSerializer = new StringSerializer

        publishToKafka(inputTopic, messages)

        val kafkaConsumerAgentOne = new KafkaConsumerAgent(kafkaConsumerOption)

        val kafkaProducerAgent = new KafkaSimpleProducerAgent(kafkaSimpleProducerOption)

        val kafkaConsumerOptionTwo: KafkaConsumerOption[String, String] =
          KafkaConsumerOption(Seq(outputTopic), "my-other-consumer")

        val kafkaConsumerAgentTwo = new KafkaConsumerAgent(kafkaConsumerOptionTwo)

        KSource.fromKafkaConsumer(kafkaConsumerAgentOne).take(recordsSize).produce(3)(kafkaProducerAgent).runWith(Sink.ignore)
        val resultFuture = KSource.fromKafkaConsumer(kafkaConsumerAgentTwo).take(recordsSize).runWith(Sink.seq)

        whenReady(resultFuture) { consumedMessages =>
          consumedMessages.map(record => (record.key, record.value)) shouldBe messages
        }
      }

    }

    "allow to produce a sequence of message with a transactional producer" in {

      val messages = Range(0, recordsSize).map { n =>
        (n.toString, n.toString)
      }

      withRunningKafka {
        Try(createCustomTopic(inputTopic))

        Try(createCustomTopic(outputTopic))

        implicit val serializer: StringSerializer = new StringSerializer

        publishToKafka(inputTopic, messages)

        val kafkaConsumerAgentOne = new KafkaConsumerAgent(kafkaConsumerOption)

        val kafkaProducerAgent = new KafkaTransactionalProducerAgent(kafkaTransactionalProducerOption)

        val kafkaTransactionConsumerOption: KafkaConsumerOption[String, String] =
          KafkaConsumerOption(Seq(outputTopic), "my-transaction-consumer")

        val kafkaTransactionConsumerAgent = new KafkaConsumerAgent(kafkaTransactionConsumerOption)

        KSource
          .fromKafkaConsumer(kafkaConsumerAgentOne)
          .take(recordsSize)
          .grouped(10)
          .produce(kafkaProducerAgent)
          .runWith(Sink.ignore)
        val resultFuture = KSource.fromKafkaConsumer(kafkaTransactionConsumerAgent).take(recordsSize).runWith(Sink.seq)

        whenReady(resultFuture) { consumedMessages =>
          consumedMessages.map(record => (record.key, record.value)) shouldBe messages
        }
      }

    }

    "allow to produce and commit message individually in a transaction with a transactional producer" in {

      val messages = Range(0, recordsSize).map { n =>
        (n.toString, n.toString)
      }

      withRunningKafka {
        Try(createCustomTopic(inputTopic))

        Try(createCustomTopic(outputTopic))

        implicit val serializer: StringSerializer = new StringSerializer

        publishToKafka(inputTopic, messages)

        val kafkaConsumerAgentOne = new KafkaConsumerAgent(kafkaConsumerOption)

        val kafkaConsumerAgentTwo = new KafkaConsumerAgent(kafkaConsumerOption)

        val kafkaProducerAgent = new KafkaTransactionalProducerAgent(kafkaTransactionalProducerOption)

        val kafkaTransactionConsumerOption: KafkaConsumerOption[String, String] =
          KafkaConsumerOption(Seq(outputTopic), "my-transaction-consumer")

        val kafkaTransactionConsumerAgent = new KafkaConsumerAgent(kafkaTransactionConsumerOption)

        val consumedRecords = KSource.fromKafkaConsumer(kafkaTransactionConsumerAgent).take(recordsSize).runWith(Sink.seq)

        for {
          _ <- KSource
            .fromKafkaConsumer(kafkaConsumerAgentOne)
            .take(recordsSize / 2)
            .produceAndCommit(kafkaProducerAgent)
            .runWith(Sink.ignore)
          _ <- kafkaConsumerAgentOne.stopConsumer
          _ <- KSource
            .fromKafkaConsumer(kafkaConsumerAgentTwo)
            .take(recordsSize / 2)
            .produceAndCommit(kafkaProducerAgent)
            .runWith(Sink.ignore)
        } yield ()

        whenReady(consumedRecords) { consumedMessages =>
          consumedMessages.map(record => (record.key, record.value)) shouldBe messages
        }
      }
    }

    "allow to produce and commit a sequence of message in a transaction with a transactional producer" in {

      val messages = Range(0, recordsSize).map { n =>
        (n.toString, n.toString)
      }

      withRunningKafka {
        Try(createCustomTopic(inputTopic))

        Try(createCustomTopic(outputTopic))

        implicit val serializer: StringSerializer = new StringSerializer

        publishToKafka(inputTopic, messages)

        val kafkaConsumerAgentOne = new KafkaConsumerAgent(kafkaConsumerOption)

        val kafkaConsumerAgentTwo = new KafkaConsumerAgent(kafkaConsumerOption)

        val kafkaProducerAgent = new KafkaTransactionalProducerAgent(kafkaTransactionalProducerOption)

        val kafkaTransactionConsumerOption: KafkaConsumerOption[String, String] =
          KafkaConsumerOption(Seq(outputTopic), "my-transaction-consumer")

        val kafkaTransactionConsumerAgent = new KafkaConsumerAgent(kafkaTransactionConsumerOption)

        val consumedRecords = KSource.fromKafkaConsumer(kafkaTransactionConsumerAgent).take(recordsSize).runWith(Sink.seq)

        for {
          _ <- KSource
            .fromKafkaConsumer(kafkaConsumerAgentOne)
            .take(recordsSize / 2)
            .grouped(10)
            .produceAndCommit(kafkaProducerAgent)
            .runWith(Sink.ignore)
          _ <- kafkaConsumerAgentOne.stopConsumer
          _ <- KSource
            .fromKafkaConsumer(kafkaConsumerAgentTwo)
            .take(recordsSize / 2)
            .grouped(10)
            .produceAndCommit(kafkaProducerAgent)
            .runWith(Sink.ignore)
        } yield ()

        whenReady(consumedRecords) { consumedMessages =>
          consumedMessages.map(record => (record.key, record.value)) shouldBe messages
        }
      }

    }

    "allow to produce and commit a sequence of message in a transaction with a transactional producer in a multi-partition topic" in {

      val messages = Range(0, recordsSize).map { n =>
        (n.toString, n.toString)
      }

      withRunningKafka {
        Try(createCustomTopic(topic = inputTopic, partitions = 3))

        Try(createCustomTopic(outputTopic))

        implicit val serializer: StringSerializer = new StringSerializer

        publishToKafka(inputTopic, messages)

        val kafkaConsumerAgentOne = new KafkaConsumerAgent(kafkaConsumerOption)

        val kafkaConsumerAgentTwo = new KafkaConsumerAgent(kafkaConsumerOption)

        val kafkaProducerAgent = new KafkaTransactionalProducerAgent(kafkaTransactionalProducerOption)

        val kafkaTransactionConsumerOption: KafkaConsumerOption[String, String] =
          KafkaConsumerOption(Seq(outputTopic), "my-transaction-consumer")

        val kafkaTransactionConsumerAgent = new KafkaConsumerAgent(kafkaTransactionConsumerOption)

        val consumedRecords = KSource.fromKafkaConsumer(kafkaTransactionConsumerAgent).take(recordsSize).runWith(Sink.seq)

        for {
          _ <- KSource
            .fromKafkaConsumer(kafkaConsumerAgentOne)
            .take(recordsSize / 2)
            .grouped(10)
            .produceAndCommit(kafkaProducerAgent)
            .runWith(Sink.ignore)
          _ <- kafkaConsumerAgentOne.stopConsumer
          _ <- KSource
            .fromKafkaConsumer(kafkaConsumerAgentTwo)
            .take(recordsSize / 2)
            .grouped(10)
            .produceAndCommit(kafkaProducerAgent)
            .runWith(Sink.ignore)
        } yield ()

        whenReady(consumedRecords) { consumedMessages =>
          consumedMessages.map(record => (record.key, record.value)).toSet shouldBe messages.toSet
        }
      }

    }

  }

}
