package org.simoexpo.as4k.producer

import akka.actor.ActorRef
import akka.pattern.AskTimeoutException
import akka.testkit.TestProbe
import org.apache.kafka.clients.producer.KafkaProducer
import org.mockito.Mockito.when
import org.scalatest.BeforeAndAfterEach
import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import org.simoexpo.as4k.producer.KafkaProducerActor.{KafkaProduceException, ProduceRecords, ProduceRecordsAndCommit}
import org.simoexpo.as4k.producer.KafkaTransactionalProducerAgent.KafkaTransactionalProducerTimeoutException
import org.simoexpo.as4k.testing.{ActorSystemSpec, BaseSpec, DataHelperSpec}

class KafkaTransactionalProducerAgentSpec
    extends BaseSpec
    with ScalaFutures
    with ActorSystemSpec
    with IntegrationPatience
    with BeforeAndAfterEach
    with DataHelperSpec {

  private val kafkaProducerOption: KafkaProducerOption[Int, String] = mock[KafkaProducerOption[Int, String]]

  when(kafkaProducerOption.topic).thenReturn("producerTopic")
  when(kafkaProducerOption.createOne()).thenReturn(mock[KafkaProducer[Int, String]])
  when(kafkaProducerOption.isTransactional).thenReturn(true)

  private val kafkaProducerActor: TestProbe = TestProbe()
  private val kafkaProducerActorRef: ActorRef = kafkaProducerActor.ref

  private val kafkaProducerAgent: KafkaTransactionalProducerAgent[Int, String] =
    new KafkaTransactionalProducerAgent(kafkaProducerOption)(system, timeout) {
      override protected val actor: ActorRef = kafkaProducerActorRef
    }

  "KafkaTransactionalProducerAgent" when {

    val topic = "topic"
    val partition = 1

    val kRecords = Range(0, 100).map(n => aKRecord(n, n, s"value$n", topic, partition)).toList

    "producing records" should {

      "produce a list of records in transaction" in {

        val produceResult = kafkaProducerAgent.produce(kRecords)

        kafkaProducerActor.expectMsg(ProduceRecords(kRecords))
        kafkaProducerActor.reply(())

        whenReady(produceResult) { records =>
          records shouldBe kRecords
        }
      }

      "fail with a KafkaProduceException if kafka producer actor fails" in {

        val produceResult = kafkaProducerAgent.produce(kRecords)

        kafkaProducerActor.expectMsg(ProduceRecords(kRecords))
        kafkaProducerActor.reply(
          akka.actor.Status.Failure(KafkaProduceException(new RuntimeException("Something bad happened!"))))

        produceResult.failed.futureValue shouldBe a[KafkaProduceException]
      }

      "fail with a KafkaTransactionalProducerTimeoutException if the no response are given before the timeout" in {

        val produceResult = kafkaProducerAgent.produce(kRecords)

        kafkaProducerActor.expectMsg(ProduceRecords(kRecords))

        produceResult.failed.futureValue shouldBe a[KafkaTransactionalProducerTimeoutException]

      }
    }

    "producing and committing records in transaction" should {

      val consumerGroup = "consumerGroup"

      "produce and commit a single record" in {

        val oneRecord = kRecords.head

        val produceResult = kafkaProducerAgent.produceAndCommit(oneRecord, consumerGroup)

        kafkaProducerActor.expectMsg(ProduceRecordsAndCommit(List(oneRecord), consumerGroup))
        kafkaProducerActor.reply(())

        whenReady(produceResult) { record =>
          record shouldBe oneRecord
        }
      }

      "produce and commit a list of records" in {

        val produceResult = kafkaProducerAgent.produceAndCommit(kRecords, consumerGroup)

        kafkaProducerActor.expectMsg(ProduceRecordsAndCommit(kRecords, consumerGroup))
        kafkaProducerActor.reply(())

        whenReady(produceResult) { records =>
          records shouldBe kRecords
        }
      }

      "fail with a KafkaProduceException if kafka producer actor fails" in {

        val produceResult = kafkaProducerAgent.produceAndCommit(kRecords, consumerGroup)

        kafkaProducerActor.expectMsg(ProduceRecordsAndCommit(kRecords, consumerGroup))
        kafkaProducerActor.reply(
          akka.actor.Status.Failure(KafkaProduceException(new RuntimeException("Something bad happened!"))))

        produceResult.failed.futureValue shouldBe a[KafkaProduceException]
      }

      "fail with a KafkaTransactionalProducerTimeoutException if the no response are given before the timeout" in {

        val oneRecord = kRecords.head
        val singleProduceResult = kafkaProducerAgent.produceAndCommit(oneRecord, consumerGroup)

        kafkaProducerActor.expectMsg(ProduceRecordsAndCommit(List(oneRecord), consumerGroup))

        val listProduceResult = kafkaProducerAgent.produceAndCommit(kRecords, consumerGroup)

        kafkaProducerActor.expectMsg(ProduceRecordsAndCommit(kRecords, consumerGroup))

        singleProduceResult.failed.futureValue shouldBe a[KafkaTransactionalProducerTimeoutException]
        listProduceResult.failed.futureValue shouldBe a[KafkaTransactionalProducerTimeoutException]

      }
    }

    "cleaning the resource" should {

      "allow to close the producer actor properly" in {

        whenReady(kafkaProducerAgent.stopProducer) { _ =>
          val exception = kafkaProducerAgent.produce(kRecords).failed.futureValue
          exception shouldBe an[KafkaTransactionalProducerTimeoutException]
          exception.getMessage should include("had already been terminated")
        }

      }

    }

  }
}
