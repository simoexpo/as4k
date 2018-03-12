package com.github.simoexpo.as4k.consumer

import akka.actor.ActorRef
import akka.testkit.TestProbe
import com.github.simoexpo.ActorSystemSpec
import com.github.simoexpo.as4k.consumer.KafkaConsumerActor._
import com.github.simoexpo.as4k.factory.{CallbackFactory, KRecord}
import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.TopicPartition
import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfterEach, Matchers, WordSpec}

class KafkaConsumerAgentSpec
    extends WordSpec
    with Matchers
    with MockitoSugar
    with ScalaFutures
    with ActorSystemSpec
    with IntegrationPatience
    with BeforeAndAfterEach {

  private val kafkaConsumerOption: KafkaConsumerOption[Int, String] = mock[KafkaConsumerOption[Int, String]]

  private val kafkaConsumerActor: TestProbe = TestProbe()
  private val kafkaConsumerActorRef: ActorRef = kafkaConsumerActor.ref

  private val PollingInterval = 200

  private val kafkaConsumerAgent: KafkaConsumerAgent[Int, String] =
    new KafkaConsumerAgent(kafkaConsumerOption, PollingInterval)(system, timeout) {
      override val actor: ActorRef = kafkaConsumerActorRef
    }

  val topic = "topic"
  val partition = 1

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

    "asking the consumer actor to synchronously commit" should {

      "commit a single ConsumerRecord" in {

        val kRecord = kRecords.head

        val recordsCommittedFuture = kafkaConsumerAgent.commit(kRecord)

        kafkaConsumerActor.expectMsg(CommitOffsetSync(List(kRecord)))
        kafkaConsumerActor.reply(())

        recordsCommittedFuture.futureValue shouldBe kRecord
      }

      "commit a list of ConsumerRecord" in {

        val recordsCommittedFuture = kafkaConsumerAgent.commit(kRecords)

        kafkaConsumerActor.expectMsg(CommitOffsetSync(kRecords))
        kafkaConsumerActor.reply(())

        recordsCommittedFuture.futureValue shouldBe kRecords
      }

      "fail with a KafkaCommitException if the consumer actor fails" in {

        val recordsCommittedFuture = kafkaConsumerAgent.commit(kRecords)

        kafkaConsumerActor.expectMsg(CommitOffsetSync(kRecords))
        kafkaConsumerActor.reply(akka.actor.Status.Failure(KafkaCommitException(new RuntimeException("Something bad happened!"))))

        recordsCommittedFuture.failed.futureValue shouldBe a[KafkaCommitException]
      }
    }

    "asking the consumer actor to asynchronously commit" should {

      val callback = CallbackFactory { (offset: Map[TopicPartition, OffsetAndMetadata], exception: Option[Exception]) =>
        exception match {
          case None     => println(s"successfully commit offset $offset")
          case Some(ex) => throw ex
        }
      }

      "commit a single ConsumerRecord" in {

        val kRecord = kRecords.head

        val recordsCommittedFuture = kafkaConsumerAgent.commitAsync(kRecord, callback)

        val actualRecords = List(kRecord)

        kafkaConsumerActor.expectMsgPF() {
          case CommitOffsetAsync(`actualRecords`, _) => ()
        }
        kafkaConsumerActor.reply(())

        recordsCommittedFuture.futureValue shouldBe kRecord
      }

      "commit a list of ConsumerRecord" in {

        val recordsCommittedFuture = kafkaConsumerAgent.commitAsync(kRecords, callback)

        kafkaConsumerActor.expectMsgPF() {
          case CommitOffsetAsync(`kRecords`, _) => ()
        }
        kafkaConsumerActor.reply(())

        recordsCommittedFuture.futureValue shouldBe kRecords
      }

      "fail with a KafkaCommitException if the consumer actor fails" in {

        val recordsCommittedFuture = kafkaConsumerAgent.commitAsync(kRecords, callback)

        kafkaConsumerActor.expectMsgPF() {
          case CommitOffsetAsync(`kRecords`, _) => ()
        }
        kafkaConsumerActor.reply(akka.actor.Status.Failure(KafkaCommitException(new RuntimeException("Something bad happened!"))))

        recordsCommittedFuture.failed.futureValue shouldBe a[KafkaCommitException]
      }
    }
  }

  private def aKRecord[K, V](offset: Long, key: K, value: V, topic: String, partition: Int) =
    KRecord(key, value, topic, partition, offset, System.currentTimeMillis())
}
