package org.simoexpo.as4k.consumer

import java.util.{Map => JavaMap}

import akka.Done
import akka.actor.Props
import akka.pattern.ask
import akka.testkit.EventFilter
import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.TopicPartition
import org.mockito.ArgumentMatchers.{any, eq => mockitoEq}
import org.mockito.Mockito.{atLeast => invokedAtLeast, _}
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.BeforeAndAfterEach
import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import org.simoexpo.as4k.consumer.KafkaConsumerActor._
import org.simoexpo.as4k.model
import org.simoexpo.as4k.model.KRecord
import org.simoexpo.as4k.testing.{ActorSystemSpec, BaseSpec, DataHelperSpec}

import scala.collection.JavaConverters._
import scala.concurrent.Future

class KafkaConsumerActorSpec
    extends BaseSpec
    with ScalaFutures
    with ActorSystemSpec
    with IntegrationPatience
    with BeforeAndAfterEach
    with DataHelperSpec {

  val topic = "topic"
  val partition = 1
  val consumerGroup = "defaultGroup"

  private val kafkaConsumerOption: KafkaConsumerOption[Int, String] = mock[KafkaConsumerOption[Int, String]]
  when(kafkaConsumerOption.topics).thenReturn(List(topic))
  when(kafkaConsumerOption.groupId).thenReturn(consumerGroup)
  private val kafkaConsumer: KafkaConsumer[Int, String] = mock[KafkaConsumer[Int, String]]

  private val PollingTimeout = 200

  private val kafkaConsumerActor = system.actorOf(Props(new KafkaConsumerActor(kafkaConsumerOption, PollingTimeout) {
    override protected val consumer = kafkaConsumer
  }))

  override def beforeEach(): Unit =
    reset(kafkaConsumer)

  "KafkaConsumerActor" when {

    val records = Range(0, 100).map(n => aConsumerRecord(n, n, s"value$n", topic, partition)).toList

    "polling for new records" should {

      "get the records from kafka consumer" in {

        val consumerRecords = new ConsumerRecords(Map((new TopicPartition(topic, partition), records.asJava)).asJava)

        val expectedKRecords =
          consumerRecords.iterator().asScala.map(record => KRecord(record, consumerGroup)).toList

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

    "ask to commit" should {

      val callback = (offsets: Map[TopicPartition, OffsetAndMetadata], exception: Option[Exception]) =>
        exception match {
          case None    => println(s"successfully commit offset $offsets")
          case Some(_) => println(s"fail commit offset $offsets")
      }

      "return immediately if an empty sequence is passed" in {

        val resultFuture = kafkaConsumerActor ? CommitOffsets(List.empty, Some(callback))

        whenReady(resultFuture) { result =>
          result shouldBe Done
        }
      }

      "commitAsync the offset of the last record in a seq, start polling until callback response and return if success" in {

        val kRecords = records.map(record => KRecord(record, consumerGroup))

        doAnswer(new Answer[Unit]() {
          override def answer(invocation: InvocationOnMock) = {
            Future {
              Thread.sleep(100)
              val offset = invocation.getArgument[JavaMap[TopicPartition, OffsetAndMetadata]](0)
              invocation.getArgument[OffsetCommitCallback](1).onComplete(offset, null)
            }
            ()
          }
        }).when(kafkaConsumer).commitAsync(any[JavaMap[TopicPartition, OffsetAndMetadata]], any[OffsetCommitCallback])

        val commitResult = kafkaConsumerActor ? CommitOffsets(kRecords, Some(callback))

        whenReady(commitResult) { _ =>
          val topicAndOffset = committableMetadata(kRecords.last)
          verify(kafkaConsumer).commitAsync(mockitoEq(topicAndOffset), any[OffsetCommitCallback])
          verify(kafkaConsumer, invokedAtLeast(1)).poll(0)
        }
      }

      "commitAsync the offset of the last record in a seq, start polling until callback response and return if failure" in {

        val kRecords = records.map(record => KRecord(record, consumerGroup))

        doAnswer(new Answer[Unit]() {
          override def answer(invocation: InvocationOnMock) = {
            Future {
              Thread.sleep(100)
              val offset = invocation.getArgument[JavaMap[TopicPartition, OffsetAndMetadata]](0)
              val exception = new RuntimeException("something bad happened!")
              invocation.getArgument[OffsetCommitCallback](1).onComplete(offset, exception)
            }
            ()
          }
        }).when(kafkaConsumer).commitAsync(any[JavaMap[TopicPartition, OffsetAndMetadata]], any[OffsetCommitCallback])

        val commitResult = kafkaConsumerActor ? CommitOffsets(kRecords, Some(callback))

        whenReady(commitResult.failed) { exception =>
          val topicAndOffset = committableMetadata(kRecords.last)
          verify(kafkaConsumer).commitAsync(mockitoEq(topicAndOffset), any[OffsetCommitCallback])
          verify(kafkaConsumer, invokedAtLeast(1)).poll(0)
          exception shouldBe a[KafkaCommitException]
        }
      }

      "fail with a KafkaCommitException if the call to commitAsync fails" in {

        val kRecords = records.map(record => KRecord(record, consumerGroup))

        when(kafkaConsumer.commitAsync(any[JavaMap[TopicPartition, OffsetAndMetadata]], any[OffsetCommitCallback]))
          .thenThrow(new RuntimeException("something bad happened!"))

        val recordsCommittedFuture = kafkaConsumerActor ? CommitOffsets(kRecords, Some(callback))

        whenReady(recordsCommittedFuture.failed) { exception =>
          exception shouldBe a[KafkaCommitException]
        }
      }
    }

    "receiving a unexpected message" should {

      "log a warning" in {
        EventFilter.warning(start = "Unexpected message:", occurrences = 1) intercept {
          kafkaConsumerActor ! "UnexpectedMessage"
        }
      }

    }
  }

}
