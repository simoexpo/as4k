package com.github.simoexpo.as4k.consumer

import java.util

import akka.pattern.ask
import com.github.simoexpo.ActorSystemSpec
import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.TopicPartition
import org.mockito.ArgumentMatchers.{any, eq => mockitoEq}
import org.mockito.Mockito._
import org.scalatest.{BeforeAndAfterEach, Matchers, WordSpec}
import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import org.scalatest.mockito.MockitoSugar
import com.github.simoexpo.as4k.consumer.KafkaConsumerActor.{CommitOffsetAsync, CommitOffsetSync, ConsumerToken}
import com.github.simoexpo.as4k.factory.KRecord

import scala.collection.JavaConverters._

class KafkaConsumerActorSpec
    extends WordSpec
    with Matchers
    with MockitoSugar
    with ScalaFutures
    with ActorSystemSpec
    with IntegrationPatience
    with BeforeAndAfterEach {

  private val kafkaConsumer: KafkaConsumer[Int, String] = mock[KafkaConsumer[Int, String]]

  private val PollingInterval = 200

  private val kafkaConsumerActor = system.actorOf(KafkaConsumerActor.props(kafkaConsumer, PollingInterval))

  override def beforeEach(): Unit =
    reset(kafkaConsumer)

  val topic = "topic"
  val partition = 1

  "KafkaConsumerActor" should {

    val records = Range(0, 100).map(n => aConsumerRecord(n, n, s"value$n")).toList

    "poll new records with the kafka consumer when receive a ConsumerToken" in {

      val consumerRecords = new ConsumerRecords(Map((new TopicPartition(topic, partition), records.asJava)).asJava)

      when(kafkaConsumer.poll(PollingInterval)).thenReturn(consumerRecords)

      val recordsConsumedFuture = kafkaConsumerActor ? ConsumerToken

      whenReady(recordsConsumedFuture) { recordsConsumed =>
        recordsConsumed.asInstanceOf[ConsumerRecords[Int, String]] shouldBe consumerRecords
      }
    }

    "allow to commit a list of ConsumerRecord synchronously" in {

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

    val callback = new OffsetCommitCallback {
      override def onComplete(offsets: util.Map[TopicPartition, OffsetAndMetadata], exception: Exception): Unit =
        exception match {
          case null => println(s"successfully commit offset $offsets")
          case ex   => throw ex
        }
    }

    "allow to commit a list of ConsumerRecord asynchronously" in {

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
  }

  private def committableMetadata[K, V](record: KRecord[K, V]) = {
    val topicPartition = new TopicPartition(record.topic, record.partition)
    val offsetAndMetadata = new OffsetAndMetadata(record.offset + 1)
    Map(topicPartition -> offsetAndMetadata).asJava
  }

  private def aConsumerRecord[K, V](offset: Long, key: K, value: V) =
    new ConsumerRecord(topic, partition, offset, key, value)
}
