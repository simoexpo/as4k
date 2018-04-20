package org.simoexpo.as4k.it

import akka.stream.scaladsl.Sink
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.common.serialization.StringSerializer
import org.scalatest.BeforeAndAfterEach
import org.scalatest.concurrent.ScalaFutures
import org.simoexpo.as4k.KSource
import org.simoexpo.as4k.KSource._
import org.simoexpo.as4k.consumer.{KafkaConsumerAgent, KafkaConsumerOption}
import org.simoexpo.as4k.it.testing.{ActorSystemSpec, BaseSpec, LooseIntegrationPatience}
import org.simoexpo.as4k.producer.{KafkaProducerOption, KafkaSimpleProducerAgent, KafkaTransactionalProducerAgent}

class KSourceIntegrationSpec
    extends BaseSpec
    with ActorSystemSpec
    with BeforeAndAfterEach
    with EmbeddedKafka
    with ScalaFutures
    with LooseIntegrationPatience {

  //Need these because apparently kafka is not always stopped properly
  override def afterEach(): Unit =
    Thread.sleep(10000)

  "KSource" should {

    val inputTopic = "in_topic"
    val outputTopic = "out_topic"

    implicit val config = EmbeddedKafkaConfig(9092, 2181)

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
        createCustomTopic(inputTopic)

        implicit val serializer: StringSerializer = new StringSerializer

        publishToKafka(inputTopic, messages)

        val kafkaConsumerAgent = new KafkaConsumerAgent(kafkaConsumerOption, 100)

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
        createCustomTopic(inputTopic)

        implicit val serializer: StringSerializer = new StringSerializer

        publishToKafka(inputTopic, messages)

        val kafkaConsumerAgentOne = new KafkaConsumerAgent(kafkaConsumerOption, 100)

        val kafkaConsumerAgentTwo = new KafkaConsumerAgent(kafkaConsumerOption, 100)

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

        whenReady(resultFuture) { result =>
          result._1.map(record => (record.key, record.value)) shouldBe messages.take(recordsSize / 2)
          result._2.map(record => (record.key, record.value)) shouldBe messages.drop(recordsSize / 2)
        }
      }
    }

    "allow to commit a sequence of message" in {

      val messages = Range(0, recordsSize).map { n =>
        (n.toString, n.toString)
      }

      withRunningKafka {
        createCustomTopic(inputTopic)

        implicit val serializer: StringSerializer = new StringSerializer

        publishToKafka(inputTopic, messages)

        val kafkaConsumerAgentOne = new KafkaConsumerAgent(kafkaConsumerOption, 100)

        val kafkaConsumerAgentTwo = new KafkaConsumerAgent(kafkaConsumerOption, 100)

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

        whenReady(resultFuture) { result =>
          result._1.flatMap(records => records.map(record => (record.key, record.value))) shouldBe messages.take(recordsSize / 2)
          result._2.flatMap(records => records.map(record => (record.key, record.value))) shouldBe messages.drop(recordsSize / 2)
        }
      }
    }

    "allow to map value of messages" in {

      val messages = Range(0, recordsSize).map { n =>
        (n.toString, n.toString)
      }

      val mapFunction: String => String = (s: String) => s + "mapped"

      val mappedValueMessages = messages.map(m => (m._1, mapFunction(m._2)))

      withRunningKafka {
        createCustomTopic(inputTopic)

        implicit val serializer: StringSerializer = new StringSerializer

        publishToKafka(inputTopic, messages)

        val kafkaConsumerAgent = new KafkaConsumerAgent(kafkaConsumerOption, 100)

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
        createCustomTopic(inputTopic)

        createCustomTopic(outputTopic)

        implicit val serializer: StringSerializer = new StringSerializer

        publishToKafka(inputTopic, messages)

        val kafkaConsumerAgentOne = new KafkaConsumerAgent(kafkaConsumerOption, 100)

        val kafkaProducerAgent = new KafkaSimpleProducerAgent(kafkaSimpleProducerOption)

        val kafkaConsumerOptionTwo: KafkaConsumerOption[String, String] =
          KafkaConsumerOption(Seq(outputTopic), "my-other-consumer")

        val kafkaConsumerAgentTwo = new KafkaConsumerAgent(kafkaConsumerOptionTwo, 100)

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
        createCustomTopic(inputTopic)

        createCustomTopic(outputTopic)

        implicit val serializer: StringSerializer = new StringSerializer

        publishToKafka(inputTopic, messages)

        val kafkaConsumerAgentOne = new KafkaConsumerAgent(kafkaConsumerOption, 100)

        val kafkaProducerAgent = new KafkaTransactionalProducerAgent(kafkaTransactionalProducerOption)

        val kafkaTransactionConsumerOption: KafkaConsumerOption[String, String] =
          KafkaConsumerOption(Seq(outputTopic), "my-transaction-consumer")

        val kafkaTransactionConsumerAgent = new KafkaConsumerAgent(kafkaTransactionConsumerOption, 100)

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
        createCustomTopic(inputTopic)

        createCustomTopic(outputTopic)

        implicit val serializer: StringSerializer = new StringSerializer

        publishToKafka(inputTopic, messages)

        val kafkaConsumerAgentOne = new KafkaConsumerAgent(kafkaConsumerOption, 100)

        val kafkaConsumerAgentTwo = new KafkaConsumerAgent(kafkaConsumerOption, 100)

        val kafkaProducerAgent = new KafkaTransactionalProducerAgent(kafkaTransactionalProducerOption)

        val kafkaTransactionConsumerOption: KafkaConsumerOption[String, String] =
          KafkaConsumerOption(Seq(outputTopic), "my-transaction-consumer")

        val kafkaTransactionConsumerAgent = new KafkaConsumerAgent(kafkaTransactionConsumerOption, 100)

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
        createCustomTopic(inputTopic)

        createCustomTopic(outputTopic)

        implicit val serializer: StringSerializer = new StringSerializer

        publishToKafka(inputTopic, messages)

        val kafkaConsumerAgentOne = new KafkaConsumerAgent(kafkaConsumerOption, 100)

        val kafkaConsumerAgentTwo = new KafkaConsumerAgent(kafkaConsumerOption, 100)

        val kafkaProducerAgent = new KafkaTransactionalProducerAgent(kafkaTransactionalProducerOption)

        val kafkaTransactionConsumerOption: KafkaConsumerOption[String, String] =
          KafkaConsumerOption(Seq(outputTopic), "my-transaction-consumer")

        val kafkaTransactionConsumerAgent = new KafkaConsumerAgent(kafkaTransactionConsumerOption, 100)

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

  }

}
