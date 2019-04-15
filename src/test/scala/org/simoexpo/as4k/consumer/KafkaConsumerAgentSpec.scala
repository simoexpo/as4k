package org.simoexpo.as4k.consumer

import akka.actor.ActorRef
import akka.testkit.{TestKitBase, TestProbe}
import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.TopicPartition
import org.mockito.Mockito._
import org.scalatest.BeforeAndAfterEach
import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import org.simoexpo.as4k.consumer.KafkaConsumerActor._
import org.simoexpo.as4k.consumer.KafkaConsumerAgent.KafkaConsumerTimeoutException
import org.simoexpo.as4k.model.NonEmptyList
import org.simoexpo.as4k.testing.{ActorSystemSpec, BaseSpec, DataHelperSpec}

import scala.concurrent.duration._
import scala.language.postfixOps

class KafkaConsumerAgentSpec
    extends BaseSpec
    with ScalaFutures
    with ActorSystemSpec
    with IntegrationPatience
    with BeforeAndAfterEach
    with DataHelperSpec
    with TestKitBase {

  private val groupId = "groupId"
  private val topic = "topic"
  private val partitions = 3
  private val pollingTimeout = 200 millis

  private val kafkaConsumerOption: KafkaConsumerOption[Int, String] = mock[KafkaConsumerOption[Int, String]]
  when(kafkaConsumerOption.groupId).thenReturn(groupId)
  when(kafkaConsumerOption.dispatcher).thenReturn(None)
  when(kafkaConsumerOption.topics).thenReturn(List(topic))
  when(kafkaConsumerOption.createOne()).thenReturn(mock[KafkaConsumer[Int, String]])
  when(kafkaConsumerOption.pollingTimeout).thenReturn(pollingTimeout)

  private val kafkaConsumerActorOne: TestProbe = TestProbe()
  private val kafkaConsumerActorTwo: TestProbe = TestProbe()
  private val kafkaConsumerActorThree: TestProbe = TestProbe()

  watch(kafkaConsumerActorOne.ref)
  watch(kafkaConsumerActorTwo.ref)
  watch(kafkaConsumerActorThree.ref)

  private val kafkaConsumerActors =
    NonEmptyList(kafkaConsumerActorOne.ref, List(kafkaConsumerActorTwo.ref, kafkaConsumerActorThree.ref))

  private val kafkaConsumerAgent: KafkaConsumerAgent[Int, String] =
    new KafkaConsumerAgent(kafkaConsumerOption)(system, timeout) {
      override val actors: NonEmptyList[ActorRef] = kafkaConsumerActors
    }

  "KafkaConsumerAgent" when {

    val kRecords = Range(0, 100).map(n => aKRecord(n, n, s"value$n", topic, n % partitions, groupId)).toList

    "creating a new instance" should {

      "fail if the number of consumers is less than 1" in {

        an[IllegalArgumentException] shouldBe thrownBy(new KafkaConsumerAgent(kafkaConsumerOption, 0))

      }
    }

    "getting new records" should {

      val kRecordsOne = kRecords.groupBy(_.metadata.partition)(0)
      val kRecordsTwo = kRecords.groupBy(_.metadata.partition)(1)
      val kRecordsThree = kRecords.groupBy(_.metadata.partition)(2)

      "retrieve new records from all the underlyings consumer actors" in {

        val recordsConsumedFuture = kafkaConsumerAgent.askForRecords

        kafkaConsumerActorOne.expectMsg(ConsumerToken)
        kafkaConsumerActorOne.reply(kRecordsOne)
        kafkaConsumerActorTwo.expectMsg(ConsumerToken)
        kafkaConsumerActorTwo.reply(kRecordsTwo)
        kafkaConsumerActorThree.expectMsg(ConsumerToken)
        kafkaConsumerActorThree.reply(kRecordsThree)

        whenReady(recordsConsumedFuture) { recordsConsumed =>
          recordsConsumed should contain theSameElementsAs kRecords
        }
      }

      "fail with a KafkaPollingException if a consumer actor fails" in {

        val recordsConsumedFuture = kafkaConsumerAgent.askForRecords

        kafkaConsumerActorOne.expectMsg(ConsumerToken)
        kafkaConsumerActorOne.reply(kRecordsOne)
        kafkaConsumerActorTwo.expectMsg(ConsumerToken)
        kafkaConsumerActorTwo.reply(
          akka.actor.Status.Failure(KafkaPollingException(new RuntimeException("Something bad happened!"))))
        kafkaConsumerActorThree.expectMsg(ConsumerToken)
        kafkaConsumerActorThree.reply(kRecordsThree)

        whenReady(recordsConsumedFuture.failed) { exception =>
          exception shouldBe a[KafkaPollingException]
        }
      }

      "fail with a KafkaConsumerTimeoutException if a consumer do not respond before the timeout" in {

        val recordsConsumedFuture = kafkaConsumerAgent.askForRecords

        kafkaConsumerActorOne.expectMsg(ConsumerToken)
        kafkaConsumerActorTwo.expectMsg(ConsumerToken)
        kafkaConsumerActorTwo.reply(kRecordsTwo)
        kafkaConsumerActorThree.expectMsg(ConsumerToken)
        kafkaConsumerActorThree.reply(kRecordsThree)

        whenReady(recordsConsumedFuture.failed) { ex =>
          ex shouldBe a[KafkaConsumerTimeoutException]
        }
      }
    }

    "committing messages" should {

      val callback = (offsets: Map[TopicPartition, OffsetAndMetadata], exception: Option[Exception]) =>
        exception match {
          case None    => println(s"successfully commit offset $offsets")
          case Some(_) => println(s"fail commit offset $offsets")
      }

      "ask the main consumer to commit a single ConsumerRecord" in {

        val kRecord = kRecords.head

        val recordsCommittedFuture = kafkaConsumerAgent.commit(kRecord)

        val actualRecords = List(kRecord)

        kafkaConsumerActorOne.expectMsgPF() {
          case CommitOffsets(`actualRecords`, None) => ()
        }
        kafkaConsumerActorOne.reply(())

        recordsCommittedFuture.futureValue shouldBe kRecord
      }

      "ask the main consumer to commit a list of ConsumerRecord" in {

        val recordsCommittedFuture = kafkaConsumerAgent.commitBatch(kRecords, Some(callback))

        kafkaConsumerActorOne.expectMsgPF() {
          case CommitOffsets(`kRecords`, Some(_)) => ()
        }
        kafkaConsumerActorOne.reply(())

        recordsCommittedFuture.futureValue shouldBe kRecords
      }

      "fail with a KafkaCommitException if the main consumer actor fails" in {

        val recordsCommittedFuture = kafkaConsumerAgent.commitBatch(kRecords)

        kafkaConsumerActorOne.expectMsgPF() {
          case CommitOffsets(`kRecords`, None) => ()
        }
        kafkaConsumerActorOne.reply(
          akka.actor.Status.Failure(KafkaCommitException(new RuntimeException("Something bad happened!"))))

        recordsCommittedFuture.failed.futureValue shouldBe a[KafkaCommitException]
      }

      "fail with a KafkaConsumerTimeoutException if the no response are given before the timeout" in {

        val kRecord = kRecords.head

        val actualRecords = List(kRecord)

        val recordCommittedFuture = kafkaConsumerAgent.commit(kRecord)
        kafkaConsumerActorOne.expectMsgPF() {
          case CommitOffsets(`actualRecords`, None) => ()
        }

        val recordsCommittedFuture = kafkaConsumerAgent.commitBatch(kRecords, Some(callback))
        kafkaConsumerActorOne.expectMsgPF() {
          case CommitOffsets(`kRecords`, Some(_)) => ()
        }

        recordCommittedFuture.failed.futureValue shouldBe a[KafkaConsumerTimeoutException]
        recordsCommittedFuture.failed.futureValue shouldBe a[KafkaConsumerTimeoutException]

      }
    }

    "cleaning the resource" should {

      "allow to close the consumer actor properly" in {

        whenReady(kafkaConsumerAgent.stopConsumer) { _ =>
          expectTerminated(kafkaConsumerActorOne.ref)
          expectTerminated(kafkaConsumerActorTwo.ref)
          expectTerminated(kafkaConsumerActorThree.ref)

          val exception = kafkaConsumerAgent.askForRecords.failed.futureValue
          exception shouldBe an[KafkaConsumerTimeoutException]
          exception.getMessage should include("had already been terminated")
        }

      }

    }
  }

}
