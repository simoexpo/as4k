package com.github.simoexpo.as4k.consumer

import java.util

import akka.actor.Props
import akka.pattern.ask
import com.github.simoexpo.as4k.DataHelperSpec
import com.github.simoexpo.{ActorSystemSpec, BaseSpec}
import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.TopicPartition
import org.mockito.ArgumentMatchers.{any, eq => mockitoEq}
import org.mockito.Mockito._
import org.scalatest.BeforeAndAfterEach
import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import com.github.simoexpo.as4k.consumer.KafkaConsumerActor._
import com.github.simoexpo.as4k.factory.KRecord

import scala.collection.JavaConverters._

class KafkaConsumerActorSpec
    extends BaseSpec
    with ScalaFutures
    with ActorSystemSpec
    with IntegrationPatience
    with BeforeAndAfterEach
    with DataHelperSpec {

  private val kafkaConsumerOption: KafkaConsumerOption[Int, String] = mock[KafkaConsumerOption[Int, String]]
  private val kafkaConsumer: KafkaConsumer[Int, String] = mock[KafkaConsumer[Int, String]]

  private val PollingTimeout = 200

  private val kafkaConsumerActor = system.actorOf(Props(new KafkaConsumerActor(kafkaConsumerOption, PollingTimeout) {
    override protected val consumer = kafkaConsumer
  }))

  override def beforeEach(): Unit =
    reset(kafkaConsumer)

  "KafkaConsumerActor" when {

    val topic = "topic"
    val partition = 1

    val records = Range(0, 100).map(n => aConsumerRecord(n, n, s"value$n", topic, partition)).toList

    "polling for new records" should {

      "get the records from kafka consumer" in {

        val consumerRecords = new ConsumerRecords(Map((new TopicPartition(topic, partition), records.asJava)).asJava)

        val expectedKRecords = consumerRecords.iterator().asScala.map(KRecord(_)).toList

        when(kafkaConsumer.poll(PollingTimeout)).thenReturn(consumerRecords)

        val recordsConsumedFuture = kafkaConsumerActor ? ConsumerToken

        whenReady(recordsConsumedFuture) { recordsConsumed =>
          recordsConsumed.asInstanceOf[List[KRecord[Int, String]]] shouldBe expectedKRecords
        }
      }

      "fail with a KafkaPollingException if the kafka consumer fails" in {

        when(kafkaConsumer.poll(PollingTimeout)).thenThrow(new RuntimeException("something bad happened!"))

        val recordsConsumedFuture = kafkaConsumerActor ? ConsumerToken

        whenReady(recordsConsumedFuture.failed) { exception =>
          exception shouldBe a[KafkaPollingException]
        }
      }
    }

    "committing new records synchronously" should {

      "call kafka consumer to commit a list of ConsumerRecord" in {

        val kRecords = records.map(KRecord(_))

        doNothing().when(kafkaConsumer).commitSync(any[Map[TopicPartition, OffsetAndMetadata]].asJava)

        val recordsCommittedFuture = kafkaConsumerActor ? CommitOffsetSync(kRecords)

        whenReady(recordsCommittedFuture) { _ =>
          kRecords.foreach { record =>
            val topicAndOffset = committableMetadata(record)
            verify(kafkaConsumer).commitSync(topicAndOffset)
          }
        }
      }

      "fail with a KafkaCommitException if the kafka consumer fails" in {

        val kRecords = records.map(KRecord(_))

        when(kafkaConsumer.commitSync(any[Map[TopicPartition, OffsetAndMetadata]].asJava))
          .thenThrow(new RuntimeException("something bad happened!"))

        val recordsCommittedFuture = kafkaConsumerActor ? CommitOffsetSync(kRecords)

        whenReady(recordsCommittedFuture.failed) { exception =>
          exception shouldBe a[KafkaCommitException]
        }
      }

    }

    "committing new records asynchronously" should {

      val callback = new OffsetCommitCallback {
        override def onComplete(offsets: util.Map[TopicPartition, OffsetAndMetadata], exception: Exception): Unit =
          exception match {
            case null => println(s"successfully commit offset $offsets")
            case ex   => throw ex
          }
      }

      "call kafka consumer to commit a list of ConsumerRecord" in {

        val kRecords = records.map(KRecord(_))

        doNothing().when(kafkaConsumer).commitAsync(any[Map[TopicPartition, OffsetAndMetadata]].asJava, any[OffsetCommitCallback])

        val recordsCommittedFuture = kafkaConsumerActor ? CommitOffsetAsync(kRecords, callback)

        whenReady(recordsCommittedFuture) { _ =>
          kRecords.foreach { record =>
            val topicAndOffset = committableMetadata(record)
            verify(kafkaConsumer).commitAsync(mockitoEq(topicAndOffset), any[OffsetCommitCallback])
          }
        }
      }

      "fail with a KafkaCommitException if the kafka consumer fails" in {

        val kRecords = records.map(KRecord(_))

        when(kafkaConsumer.commitAsync(any[Map[TopicPartition, OffsetAndMetadata]].asJava, any[OffsetCommitCallback]))
          .thenThrow(new RuntimeException("something bad happened!"))

        val recordsCommittedFuture = kafkaConsumerActor ? CommitOffsetAsync(kRecords, callback)

        whenReady(recordsCommittedFuture.failed) { exception =>
          exception shouldBe a[KafkaCommitException]
        }
      }
    }
  }

}
