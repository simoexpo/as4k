package org.simoexpo.as4k.producer

import akka.actor.ActorRef
import akka.pattern.AskTimeoutException
import akka.testkit.TestProbe
import org.apache.kafka.clients.producer.KafkaProducer
import org.mockito.Mockito.when
import org.scalatest.BeforeAndAfterEach
import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import org.simoexpo.as4k.producer.KafkaProducerActor.{KafkaProduceException, ProduceRecord}
import org.simoexpo.as4k.producer.KafkaSimpleProducerAgent.KafkaSimpleProducerTimeoutException
import org.simoexpo.as4k.testing.{ActorSystemSpec, BaseSpec, DataHelperSpec}

class KafkaSimpleProducerAgentSpec
    extends BaseSpec
    with ScalaFutures
    with ActorSystemSpec
    with IntegrationPatience
    with BeforeAndAfterEach
    with DataHelperSpec {

  private val kafkaProducerOption: KafkaProducerOption[Int, String] = mock[KafkaProducerOption[Int, String]]

  when(kafkaProducerOption.createOne()).thenReturn(mock[KafkaProducer[Int, String]])
  when(kafkaProducerOption.isTransactional).thenReturn(false)

  private val kafkaProducerActor: TestProbe = TestProbe()
  private val kafkaProducerActorRef: ActorRef = kafkaProducerActor.ref

  private val kafkaProducerAgent: KafkaSimpleProducerAgent[Int, String] =
    new KafkaSimpleProducerAgent(kafkaProducerOption)(system, timeout) {
      override protected val actor: ActorRef = kafkaProducerActorRef
    }

  "KafkaSimpleProducerAgent" when {

    val inTopic = "input_topic"
    val outTopic = "output_topic"
    val partition = 0

    val kRecord = aKRecord(0, 0, "value0", inTopic, partition, "defaultGroup")

    "producing records" should {

      "produce a single record" in {

        val produceResult = kafkaProducerAgent.produce(kRecord, outTopic)

        kafkaProducerActor.expectMsg(ProduceRecord(kRecord, outTopic))
        kafkaProducerActor.reply(())

        whenReady(produceResult) { record =>
          record shouldBe kRecord
        }

      }

      "fail with a KafkaProduceException if kafka producer actor fails" in {

        val produceResult = kafkaProducerAgent.produce(kRecord, outTopic)

        kafkaProducerActor.expectMsg(ProduceRecord(kRecord, outTopic))
        kafkaProducerActor.reply(
          akka.actor.Status.Failure(KafkaProduceException(new RuntimeException("Something bad happened!"))))

        produceResult.failed.futureValue shouldBe a[KafkaProduceException]
      }

      "fail with a KafkaSimpleProducerTimeoutException if the no response are given before the timeout" in {

        val produceResult = kafkaProducerAgent.produce(kRecord, outTopic)
        kafkaProducerActor.expectMsg(ProduceRecord(kRecord, outTopic))

        produceResult.failed.futureValue shouldBe a[KafkaSimpleProducerTimeoutException]
      }
    }

    "cleaning the resource" should {

      "allow to close the producer actor properly" in {

        whenReady(kafkaProducerAgent.stopProducer) { _ =>
          val exception = kafkaProducerAgent.produce(kRecord, outTopic).failed.futureValue
          exception shouldBe an[KafkaSimpleProducerTimeoutException]
          exception.getMessage should include("had already been terminated")
        }

      }

    }

  }
}
