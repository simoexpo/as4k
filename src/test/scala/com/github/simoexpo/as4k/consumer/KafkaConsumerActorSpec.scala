package com.github.simoexpo.as4k.consumer

import java.util.{Map => JavaMap}

import akka.Done
import akka.actor.Props
import akka.pattern.ask
import akka.testkit.TestActors
import com.github.simoexpo.as4k.consumer.KafkaConsumerActor._
import com.github.simoexpo.as4k.helper.DataHelperSpec
import com.github.simoexpo.as4k.model.KRecord
import com.github.simoexpo.{ActorSystemSpec, BaseSpec}
import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.TopicPartition
import org.mockito.ArgumentMatchers.{any, eq => mockitoEq}
import org.mockito.Mockito.{atLeast => invokedAtLeast, _}
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.BeforeAndAfterEach
import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}

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

  private val kafkaConsumerOption: KafkaConsumerOption[Int, String] = mock[KafkaConsumerOption[Int, String]]
  when(kafkaConsumerOption.topics).thenReturn(Some(List(topic)))
  private val kafkaConsumer: KafkaConsumer[Int, String] = mock[KafkaConsumer[Int, String]]

  private val PollingTimeout = 200

  private val kafkaConsumerActor = system.actorOf(Props(new KafkaConsumerActor(kafkaConsumerOption, PollingTimeout) {
    override protected val consumer = kafkaConsumer
  }))

  val sink = system.actorOf(TestActors.blackholeProps)

  override def beforeEach(): Unit =
    reset(kafkaConsumer)

  "KafkaConsumerActor" when {

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

        val kRecords = records.map(KRecord(_))

        doAnswer(new Answer[Unit]() {
          override def answer(invocation: InvocationOnMock) =
            Future {
              val offset = invocation.getArgument[JavaMap[TopicPartition, OffsetAndMetadata]](0)
              invocation.getArgument[OffsetCommitCallback](1).onComplete(offset, null)
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

        val kRecords = records.map(KRecord(_))

        doAnswer(new Answer[Unit]() {
          override def answer(invocation: InvocationOnMock) =
            Future {
              val offset = invocation.getArgument[JavaMap[TopicPartition, OffsetAndMetadata]](0)
              val exception = new RuntimeException("something bad happened!")
              invocation.getArgument[OffsetCommitCallback](1).onComplete(offset, exception)
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

        val kRecords = records.map(KRecord(_))

        when(kafkaConsumer.commitAsync(any[JavaMap[TopicPartition, OffsetAndMetadata]], any[OffsetCommitCallback]))
          .thenThrow(new RuntimeException("something bad happened!"))

        val recordsCommittedFuture = kafkaConsumerActor ? CommitOffsets(kRecords, Some(callback))

        whenReady(recordsCommittedFuture.failed) { exception =>
          exception shouldBe a[KafkaCommitException]
        }
      }
    }
  }

}
