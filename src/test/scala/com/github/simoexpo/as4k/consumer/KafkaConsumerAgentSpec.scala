package com.github.simoexpo.as4k.consumer

import akka.actor.ActorRef
import akka.testkit.TestProbe
import com.github.simoexpo.as4k.consumer.KafkaConsumerActor._
import com.github.simoexpo.as4k.testing.{ActorSystemSpec, BaseSpec, DataHelperSpec}
import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.TopicPartition
import org.mockito.Mockito._
import org.scalatest.BeforeAndAfterEach
import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}

class KafkaConsumerAgentSpec
    extends BaseSpec
    with ScalaFutures
    with ActorSystemSpec
    with IntegrationPatience
    with BeforeAndAfterEach
    with DataHelperSpec {

  val groupId = "groupId"
  val topic = "topic"
  val partition = 1

  private val kafkaConsumerOption: KafkaConsumerOption[Int, String] = mock[KafkaConsumerOption[Int, String]]
  when(kafkaConsumerOption.groupId).thenReturn(Some(groupId))
  when(kafkaConsumerOption.dispatcher).thenReturn(None)
  when(kafkaConsumerOption.topics).thenReturn(List(topic))
  when(kafkaConsumerOption.createOne()).thenReturn(mock[KafkaConsumer[Int, String]])

  private val kafkaConsumerActor: TestProbe = TestProbe()
  private val kafkaConsumerActorRef: ActorRef = kafkaConsumerActor.ref

  private val PollingTimeout = 200

  private val kafkaConsumerAgent: KafkaConsumerAgent[Int, String] =
    new KafkaConsumerAgent(kafkaConsumerOption, PollingTimeout)(system, timeout) {
      override val actor: ActorRef = kafkaConsumerActorRef
    }

  "KafkaConsumerAgent" when {

    val kRecords = Range(0, 100).map(n => aKRecord(n, n, s"value$n", topic, partition)).toList

    "asking the consumer actor to poll" should {

      "retrieve new records" in {

        val recordsConsumedFuture = kafkaConsumerAgent.askForRecords(ConsumerToken)

        kafkaConsumerActor.expectMsg(ConsumerToken)
        kafkaConsumerActor.reply(kRecords)

        whenReady(recordsConsumedFuture) { recordsConsumed =>
          recordsConsumed shouldBe kRecords
        }
      }

      "fail with a KafkaPollingException if the consumer actor fails" in {

        val recordsConsumedFuture = kafkaConsumerAgent.askForRecords(ConsumerToken)

        kafkaConsumerActor.expectMsg(ConsumerToken)
        kafkaConsumerActor.reply(
          akka.actor.Status.Failure(KafkaPollingException(new RuntimeException("Something bad happened!"))))

        whenReady(recordsConsumedFuture.failed) { exception =>
          exception shouldBe a[KafkaPollingException]
        }
      }
    }

    "asking the consumer actor to commit" should {

      val callback = (offsets: Map[TopicPartition, OffsetAndMetadata], exception: Option[Exception]) =>
        exception match {
          case None    => println(s"successfully commit offset $offsets")
          case Some(_) => println(s"fail commit offset $offsets")
      }

      "commit a single ConsumerRecord" in {

        val kRecord = kRecords.head

        val recordsCommittedFuture = kafkaConsumerAgent.commit(kRecord)

        val actualRecords = List(kRecord)

        kafkaConsumerActor.expectMsgPF() {
          case CommitOffsets(`actualRecords`, None) => ()
        }
        kafkaConsumerActor.reply(())

        recordsCommittedFuture.futureValue shouldBe kRecord
      }

      "commit a list of ConsumerRecord" in {

        val recordsCommittedFuture = kafkaConsumerAgent.commitBatch(kRecords, Some(callback))

        kafkaConsumerActor.expectMsgPF() {
          case CommitOffsets(`kRecords`, Some(_)) => ()
        }
        kafkaConsumerActor.reply(())

        recordsCommittedFuture.futureValue shouldBe kRecords
      }

      "fail with a KafkaCommitException if the consumer actor fails" in {

        val recordsCommittedFuture = kafkaConsumerAgent.commitBatch(kRecords)

        kafkaConsumerActor.expectMsgPF() {
          case CommitOffsets(`kRecords`, None) => ()
        }
        kafkaConsumerActor.reply(akka.actor.Status.Failure(KafkaCommitException(new RuntimeException("Something bad happened!"))))

        recordsCommittedFuture.failed.futureValue shouldBe a[KafkaCommitException]
      }
    }
  }

}
