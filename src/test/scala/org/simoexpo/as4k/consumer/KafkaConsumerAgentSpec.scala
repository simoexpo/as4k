package org.simoexpo.as4k.consumer

import akka.actor.ActorRef
import akka.testkit.TestProbe
import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.TopicPartition
import org.mockito.Mockito._
import org.scalatest.BeforeAndAfterEach
import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import org.simoexpo.as4k.consumer.KafkaConsumerActor._
import org.simoexpo.as4k.consumer.KafkaConsumerAgent.KafkaConsumerTimeoutException
import org.simoexpo.as4k.testing.{ActorSystemSpec, BaseSpec, DataHelperSpec}

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
  private val pollingTimeout = 200

  private val kafkaConsumerOption: KafkaConsumerOption[Int, String] = mock[KafkaConsumerOption[Int, String]]
  when(kafkaConsumerOption.groupId).thenReturn(groupId)
  when(kafkaConsumerOption.dispatcher).thenReturn(None)
  when(kafkaConsumerOption.topics).thenReturn(List(topic))
  when(kafkaConsumerOption.createOne()).thenReturn(mock[KafkaConsumer[Int, String]])
  when(kafkaConsumerOption.pollingTimeout).thenReturn(pollingTimeout)

  private val kafkaConsumerActor: TestProbe = TestProbe()
  private val kafkaConsumerActorRef: ActorRef = kafkaConsumerActor.ref

  private val kafkaConsumerAgent: KafkaConsumerAgent[Int, String] =
    new KafkaConsumerAgent(kafkaConsumerOption)(system, timeout) {
      override val actor: ActorRef = kafkaConsumerActorRef
    }

  "KafkaConsumerAgent" when {

    val kRecords = Range(0, 100).map(n => aKRecord(n, n, s"value$n", topic, partition, groupId)).toList

    "asking the consumer actor to poll" should {

      "retrieve new records" in {

        val recordsConsumedFuture = kafkaConsumerAgent.askForRecords

        kafkaConsumerActor.expectMsg(ConsumerToken)
        kafkaConsumerActor.reply(kRecords)

        whenReady(recordsConsumedFuture) { recordsConsumed =>
          recordsConsumed shouldBe kRecords
        }
      }

      "fail with a KafkaPollingException if the consumer actor fails" in {

        val recordsConsumedFuture = kafkaConsumerAgent.askForRecords

        kafkaConsumerActor.expectMsg(ConsumerToken)
        kafkaConsumerActor.reply(
          akka.actor.Status.Failure(KafkaPollingException(new RuntimeException("Something bad happened!"))))

        whenReady(recordsConsumedFuture.failed) { exception =>
          exception shouldBe a[KafkaPollingException]
        }
      }

      "fail with a KafkaConsumerTimeoutException if the no response are given before the timeout" in {

        val recordsConsumedFuture = kafkaConsumerAgent.askForRecords

        kafkaConsumerActor.expectMsg(ConsumerToken)

        whenReady(recordsConsumedFuture.failed) { ex =>
          ex shouldBe a[KafkaConsumerTimeoutException]
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

      "fail with a KafkaCosnumerTimeoutException if the no response are given before the timeout" in {

        val kRecord = kRecords.head

        val actualRecords = List(kRecord)

        val recordCommittedFuture = kafkaConsumerAgent.commit(kRecord)
        kafkaConsumerActor.expectMsgPF() {
          case CommitOffsets(`actualRecords`, None) => ()
        }

        val recordsCommittedFuture = kafkaConsumerAgent.commitBatch(kRecords, Some(callback))
        kafkaConsumerActor.expectMsgPF() {
          case CommitOffsets(`kRecords`, Some(_)) => ()
        }

        recordCommittedFuture.failed.futureValue shouldBe a[KafkaConsumerTimeoutException]
        recordsCommittedFuture.failed.futureValue shouldBe a[KafkaConsumerTimeoutException]

      }
    }

    "cleaning the resource" should {

      "allow to close the consumer actor properly" in {

        whenReady(kafkaConsumerAgent.stopConsumer) { _ =>
          val exception = kafkaConsumerAgent.askForRecords.failed.futureValue
          exception shouldBe an[KafkaConsumerTimeoutException]
          exception.getMessage should include("had already been terminated")
        }

      }

    }
  }

}
