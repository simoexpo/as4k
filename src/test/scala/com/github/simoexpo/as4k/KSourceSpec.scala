package com.github.simoexpo.as4k

import akka.stream.scaladsl.Sink
import com.github.simoexpo.ActorSystemSpec
import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.TopicPartition
import org.mockito.Mockito.{atLeast => invokedAtLeast, _}
import org.scalatest.{BeforeAndAfterEach, Matchers, WordSpec}
import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import org.scalatest.mockito.MockitoSugar
import org.mockito.ArgumentMatchers.{any, eq => mockitoEq}
import com.github.simoexpo.as4k.KSource._
import com.github.simoexpo.as4k.consumer.KafkaConsumerActor.ConsumerToken
import com.github.simoexpo.as4k.consumer.KafkaConsumerAgent
import com.github.simoexpo.as4k.factory.{KRecord, OffsetCommitCallbackFactory}
import org.mockito.invocation.InvocationOnMock

import scala.collection.JavaConverters._
import scala.concurrent.Future

class KSourceSpec
    extends WordSpec
    with Matchers
    with MockitoSugar
    with ScalaFutures
    with ActorSystemSpec
    with IntegrationPatience
    with BeforeAndAfterEach {

  private val kafkaConsumerAgent: KafkaConsumerAgent[Int, String] = mock[KafkaConsumerAgent[Int, String]]

  override def beforeEach(): Unit =
    reset(kafkaConsumerAgent)

  val topic = "topic"
  val partition = 1

  "KSource" should {

    val records1 = Range(0, 100).map(n => aConsumerRecord(n, n, s"value$n")).toList.asJava
    val records2 = Range(100, 200).map(n => aConsumerRecord(n, n, s"value$n")).toList.asJava
    val records3 = Range(200, 220).map(n => aConsumerRecord(n, n, s"value$n")).toList.asJava

    "produce a Source from the records consumed by a kafka consumer" in {

      val totalRecordsSize = Seq(records1, records2, records3).map(_.size).sum
      val expectedRecords = Seq(records1, records2, records3).flatMap(_.asScala).map(KRecord(_))

      when(kafkaConsumerAgent.askForRecords(ConsumerToken))
        .thenReturn(Future.successful(new ConsumerRecords(Map((new TopicPartition(topic, partition), records1)).asJava)))
        .thenReturn(Future.successful(new ConsumerRecords(Map((new TopicPartition(topic, partition), records2)).asJava)))
        .thenReturn(Future.successful(new ConsumerRecords(Map((new TopicPartition(topic, partition), records3)).asJava)))

      val recordsConsumed = KSource.fromKafkaConsumer(kafkaConsumerAgent).take(totalRecordsSize).runWith(Sink.seq)

      whenReady(recordsConsumed) { records =>
        records.size shouldBe totalRecordsSize
        records.toList shouldBe expectedRecords

        verify(kafkaConsumerAgent, invokedAtLeast(3)).askForRecords(ConsumerToken)

      }
    }

    "not end when doesn't receive records when asking the kafka consumer agent" in {

      val totalRecordsSize = Seq(records1, records2).map(_.size).sum
      val expectedRecords = Seq(records1, records2).flatMap(_.asScala).map(KRecord(_))

      val emptyRecords =
        new ConsumerRecords(Map((new TopicPartition(topic, partition), List.empty[ConsumerRecord[Int, String]].asJava)).asJava)

      when(kafkaConsumerAgent.askForRecords(ConsumerToken))
        .thenReturn(Future.successful(new ConsumerRecords(Map((new TopicPartition(topic, partition), records1)).asJava)))
        .thenReturn(Future.successful(emptyRecords))
        .thenReturn(Future.successful(new ConsumerRecords(Map((new TopicPartition(topic, partition), records2)).asJava)))

      val recordsConsumed = KSource.fromKafkaConsumer(kafkaConsumerAgent).take(totalRecordsSize).runWith(Sink.seq)

      whenReady(recordsConsumed) { records =>
        records.size shouldBe totalRecordsSize
        records.toList shouldBe expectedRecords

        verify(kafkaConsumerAgent, invokedAtLeast(3)).askForRecords(ConsumerToken)

      }
    }

    "call synchronously commit on KafkaConsumerAgent for a single ConsumerRecord" in {

      val totalRecordsSize = records1.size
      val expectedRecords = records1.asScala.map(KRecord(_))

      when(kafkaConsumerAgent.askForRecords(ConsumerToken))
        .thenReturn(Future.successful(new ConsumerRecords(Map((new TopicPartition(topic, partition), records1)).asJava)))
      when(kafkaConsumerAgent.commit(any[KRecord[Int, String]])).thenAnswer((invocation: InvocationOnMock) => {
        Future.successful(invocation.getArgument[ConsumerRecord[Int, String]](0))
      })

      val recordConsumed =
        KSource.fromKafkaConsumer(kafkaConsumerAgent).take(totalRecordsSize).commit(kafkaConsumerAgent).runWith(Sink.seq)

      whenReady(recordConsumed) { _ =>
        verify(kafkaConsumerAgent, atLeastOnce()).askForRecords(ConsumerToken)
        expectedRecords.foreach { record =>
          verify(kafkaConsumerAgent).commit(record)
        }
      }
    }

    "call synchronously commit on KafkaConsumerAgent for a list of ConsumerRecord" in {

      val totalRecordsSize = records1.size
      val groupSize = 10
      val expectedRecords = records1.asScala.map(KRecord(_))

      when(kafkaConsumerAgent.askForRecords(ConsumerToken))
        .thenReturn(Future.successful(new ConsumerRecords(Map((new TopicPartition(topic, partition), records1)).asJava)))
      when(kafkaConsumerAgent.commit(any[Seq[KRecord[Int, String]]])).thenAnswer((invocation: InvocationOnMock) => {
        Future.successful(invocation.getArgument[Seq[ConsumerRecord[Int, String]]](0))
      })

      val recordConsumed =
        KSource
          .fromKafkaConsumer(kafkaConsumerAgent)
          .take(totalRecordsSize)
          .grouped(groupSize)
          .commit(kafkaConsumerAgent)
          .mapConcat(_.toList)
          .runWith(Sink.seq)

      whenReady(recordConsumed) { _ =>
        verify(kafkaConsumerAgent, atLeastOnce()).askForRecords(ConsumerToken)
        expectedRecords.grouped(10).foreach { recordsGroup =>
          verify(kafkaConsumerAgent).commit(recordsGroup)
        }
      }
    }

    val callback = OffsetCommitCallbackFactory { (offset: Map[TopicPartition, OffsetAndMetadata], exception: Option[Exception]) =>
      exception match {
        case None     => println(s"successfully commit offset $offset")
        case Some(ex) => throw ex
      }
    }

    "call asynchronously commit on KafkaConsumerAgent for a single ConsumerRecord" in {

      val totalRecordsSize = records1.size
      val expectedRecords = records1.asScala.map(KRecord(_))

      when(kafkaConsumerAgent.askForRecords(ConsumerToken))
        .thenReturn(Future.successful(new ConsumerRecords(Map((new TopicPartition(topic, partition), records1)).asJava)))
      when(kafkaConsumerAgent.commitAsync(any[KRecord[Int, String]], mockitoEq(callback)))
        .thenAnswer((invocation: InvocationOnMock) => {
          Future.successful(invocation.getArgument[ConsumerRecord[Int, String]](0))
        })

      val recordConsumed =
        KSource
          .fromKafkaConsumer(kafkaConsumerAgent)
          .take(totalRecordsSize)
          .commitAsync(kafkaConsumerAgent, callback)
          .runWith(Sink.seq)

      whenReady(recordConsumed) { _ =>
        verify(kafkaConsumerAgent, atLeastOnce()).askForRecords(ConsumerToken)
        expectedRecords.foreach { record =>
          verify(kafkaConsumerAgent).commitAsync(record, callback)
        }
      }
    }

    "call asynchronously commit on KafkaConsumerAgent for a list of ConsumerRecord" in {

      val totalRecordsSize = records1.size
      val groupSize = 10
      val expectedRecords = records1.asScala.map(KRecord(_))

      when(kafkaConsumerAgent.askForRecords(ConsumerToken))
        .thenReturn(Future.successful(new ConsumerRecords(Map((new TopicPartition(topic, partition), records1)).asJava)))
      when(kafkaConsumerAgent.commitAsync(any[Seq[KRecord[Int, String]]], mockitoEq(callback)))
        .thenAnswer((invocation: InvocationOnMock) => {
          Future.successful(invocation.getArgument[Seq[ConsumerRecord[Int, String]]](0))
        })

      val recordConsumed =
        KSource
          .fromKafkaConsumer(kafkaConsumerAgent)
          .take(totalRecordsSize)
          .grouped(groupSize)
          .commitAsync(kafkaConsumerAgent, callback)
          .mapConcat(_.toList)
          .runWith(Sink.seq)

      whenReady(recordConsumed) { _ =>
        verify(kafkaConsumerAgent, atLeastOnce()).askForRecords(ConsumerToken)
        expectedRecords.grouped(10).foreach { recordsGroup =>
          verify(kafkaConsumerAgent).commitAsync(recordsGroup, callback)
        }
      }
    }
  }

  private def aConsumerRecord[K, V](offset: Long, key: K, value: V) =
    new ConsumerRecord(topic, partition, offset, key, value)
}
