package com.github.simoexpo.as4k.consumer

import akka.testkit.TestProbe
import akka.actor.ActorRef
import com.github.simoexpo.ActorSystemSpec
import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.TopicPartition
import org.scalatest.{BeforeAndAfterEach, Matchers, WordSpec}
import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import org.scalatest.mockito.MockitoSugar
import com.github.simoexpo.as4k.consumer.KafkaConsumerActor.{CommitOffsetAsync, CommitOffsetSync, ConsumerToken}
import com.github.simoexpo.as4k.factory.{KRecord, CallbackFactory}

import scala.collection.JavaConverters._

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

  "KafkaConsumerAgent" should {

    val kRecords = Range(0, 100).map(n => aKRecord(n, n, s"value$n", topic, partition)).toList

    "ask the consumer actor to poll for new records" in {

      val recordsConsumedFuture = kafkaConsumerAgent.askForRecords(ConsumerToken)

      kafkaConsumerActor.expectMsg(ConsumerToken)
      kafkaConsumerActor.reply(kRecords)

      whenReady(recordsConsumedFuture) { recordsConsumed =>
        recordsConsumed shouldBe kRecords
      }
    }

    "ask the consumer actor to commit a single ConsumerRecord synchronously" in {

      val kRecord = kRecords.head

      val recordsCommittedFuture = kafkaConsumerAgent.commit(kRecord)

      kafkaConsumerActor.expectMsg(CommitOffsetSync(List(kRecord)))
      kafkaConsumerActor.reply(())

      recordsCommittedFuture.futureValue shouldBe kRecord
    }

    "ask the consumer actor to commit a list of ConsumerRecord synchronously" in {

      val recordsCommittedFuture = kafkaConsumerAgent.commit(kRecords)

      kafkaConsumerActor.expectMsg(CommitOffsetSync(kRecords))
      kafkaConsumerActor.reply(())

      recordsCommittedFuture.futureValue shouldBe kRecords
    }

    val callback = CallbackFactory { (offset: Map[TopicPartition, OffsetAndMetadata], exception: Option[Exception]) =>
      exception match {
        case None     => println(s"successfully commit offset $offset")
        case Some(ex) => throw ex
      }
    }

    "ask the consumer actor to commit a single ConsumerRecord asynchronously" in {

      val kRecord = kRecords.head

      val recordsCommittedFuture = kafkaConsumerAgent.commitAsync(kRecord, callback)

      val actualRecords = List(kRecord)

      kafkaConsumerActor.expectMsgPF() {
        case CommitOffsetAsync(`actualRecords`, _) => ()
      }
      kafkaConsumerActor.reply(())

      recordsCommittedFuture.futureValue shouldBe kRecord
    }

    "ask the consumer actor to commit a list of ConsumerRecord asynchronously" in {

      val recordsCommittedFuture = kafkaConsumerAgent.commitAsync(kRecords, callback)

      kafkaConsumerActor.expectMsgPF() {
        case CommitOffsetAsync(`kRecords`, _) => ()
      }
      kafkaConsumerActor.reply(())

      recordsCommittedFuture.futureValue shouldBe kRecords
    }
  }

  private def aKRecord[K, V](offset: Long, key: K, value: V, topic: String, partition: Int) =
    KRecord(key, value, topic, partition, offset, System.currentTimeMillis())
}
