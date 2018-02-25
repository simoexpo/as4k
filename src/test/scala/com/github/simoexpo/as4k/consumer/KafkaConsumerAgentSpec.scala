package com.github.simoexpo.as4k.consumer

import akka.testkit.TestProbe
import akka.actor.ActorRef
import com.github.simoexpo.ActorSystemSpec
import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.TopicPartition
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.scalatest.{BeforeAndAfterEach, Matchers, WordSpec}
import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import org.scalatest.mockito.MockitoSugar
import com.github.simoexpo.as4k.consumer.KafkaConsumerActor.{CommitOffsetAsync, CommitOffsetSync, ConsumerToken}
import com.github.simoexpo.as4k.factory.{KRecord, OffsetCommitCallbackFactory}

import scala.collection.JavaConverters._

class KafkaConsumerAgentSpec
    extends WordSpec
    with Matchers
    with MockitoSugar
    with ScalaFutures
    with ActorSystemSpec
    with IntegrationPatience
    with BeforeAndAfterEach {

  private val kafkaConsumer: KafkaConsumer[Int, String] = mock[KafkaConsumer[Int, String]]

  private val kafkaConsumerActor: TestProbe = TestProbe()
  private val kafkaConsumerActorRef: ActorRef = kafkaConsumerActor.ref

  private val PollingInterval = 200

  private val kafkaConsumerAgent: KafkaConsumerAgent[Int, String] =
    new KafkaConsumerAgent(kafkaConsumer, PollingInterval)(system, timeout) {
      override val actor: ActorRef = kafkaConsumerActorRef
    }

  override def beforeEach(): Unit =
    reset(kafkaConsumer)

  val topic = "topic"
  val partition = 1

  "KafkaConsumerAgent" should {

    val consumerRecordList = Range(0, 100).map(n => aConsumerRecord(n, n, s"value$n")).toList

    "ask the consumer actor to poll for new records" in {

      val consumerRecords = new ConsumerRecords(Map((new TopicPartition(topic, partition), consumerRecordList.asJava)).asJava)

      val recordsConsumedFuture =
        kafkaConsumerAgent.askForRecords(ConsumerToken).map(_.asInstanceOf[ConsumerRecords[Int, String]])

      kafkaConsumerActor.expectMsg(ConsumerToken)
      kafkaConsumerActor.reply(consumerRecords)

      whenReady(recordsConsumedFuture) { recordsConsumed =>
        recordsConsumed shouldBe consumerRecords
        verifyZeroInteractions(kafkaConsumer)
      }
    }

    "ask the consumer actor to commit a single ConsumerRecord synchronously" in {

      val kRecord = KRecord(consumerRecordList.head)

      val recordsCommittedFuture = kafkaConsumerAgent.commit(kRecord)

      kafkaConsumerActor.expectMsg(CommitOffsetSync(List(kRecord)))
      kafkaConsumerActor.reply(())

      whenReady(recordsCommittedFuture) { _ =>
        verifyZeroInteractions(kafkaConsumer)
      }
    }

    "ask the consumer actor to commit a list of ConsumerRecord synchronously" in {

      val kRecords = consumerRecordList.map(KRecord(_))

      doNothing().when(kafkaConsumer).commitSync(any[Map[TopicPartition, OffsetAndMetadata]].asJava)

      val recordsCommittedFuture = kafkaConsumerAgent.commit(kRecords)

      kafkaConsumerActor.expectMsg(CommitOffsetSync(kRecords))
      kafkaConsumerActor.reply(())

      whenReady(recordsCommittedFuture) { _ =>
        verifyZeroInteractions(kafkaConsumer)
      }
    }

    val callback = OffsetCommitCallbackFactory { (offset: Map[TopicPartition, OffsetAndMetadata], exception: Option[Exception]) =>
      exception match {
        case None     => println(s"successfully commit offset $offset")
        case Some(ex) => throw ex
      }
    }

    "ask the consumer actor to commit a single ConsumerRecord asynchronously" in {

      val kRecord = KRecord(consumerRecordList.head)

      val recordsCommittedFuture = kafkaConsumerAgent.commitAsync(kRecord, callback)

      val actualRecords = List(kRecord)

      kafkaConsumerActor.expectMsgPF() {
        case CommitOffsetAsync(`actualRecords`, _) => ()
      }
      kafkaConsumerActor.reply(())

      whenReady(recordsCommittedFuture) { _ =>
        verifyZeroInteractions(kafkaConsumer)
      }
    }

    "ask the consumer actor to commit a list of ConsumerRecord asynchronously" in {

      val kRecords = consumerRecordList.map(KRecord(_))

      val recordsCommittedFuture = kafkaConsumerAgent.commitAsync(kRecords, callback)

      kafkaConsumerActor.expectMsgPF() {
        case CommitOffsetAsync(`kRecords`, _) => ()
      }
      kafkaConsumerActor.reply(())

      whenReady(recordsCommittedFuture) { _ =>
        verifyZeroInteractions(kafkaConsumer)
      }
    }
  }

  private def aConsumerRecord[K, V](offset: Long, key: K, value: V) =
    new ConsumerRecord(topic, partition, offset, key, value)
}
