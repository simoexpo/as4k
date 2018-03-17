package com.github.simoexpo.as4k

import akka.stream.scaladsl.Source
import com.github.simoexpo.as4k.consumer.KafkaConsumerAgent
import com.github.simoexpo.as4k.producer.KafkaProducerAgent
import com.github.simoexpo.{ActorSystemSpec, BaseSpec}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import org.mockito.Mockito._

import scala.concurrent.Future

class KSinkSpec
    extends BaseSpec
    with ScalaFutures
    with ActorSystemSpec
    with IntegrationPatience
    with BeforeAndAfterEach
    with DataHelperSpec {

  private val kafkaConsumerAgent: KafkaConsumerAgent[Int, String] = mock[KafkaConsumerAgent[Int, String]]
  private val kafkaProducerAgent: KafkaProducerAgent[Int, String] = mock[KafkaProducerAgent[Int, String]]

  override def beforeEach(): Unit =
    reset(kafkaConsumerAgent, kafkaProducerAgent)

  "KSink" when {

    val topic = "topic"
    val partition = 1

    val kRecords = Range(0, 100).map(n => aKRecord(n, n, s"value$n", topic, partition)).toList

    "producing records on a topic" should {

      "produce single KRecord" in {

        kRecords.foreach { record =>
          when(kafkaProducerAgent.produce(record)).thenReturn(Future.successful(record))
        }

        val result = Source.fromIterator(() => kRecords.iterator).runWith(KSink.produce(kafkaProducerAgent))

        whenReady(result) { _ =>
          kRecords.foreach { record =>
            verify(kafkaProducerAgent).produce(record)
          }
        }

      }

      "produce a list of KRecord" in {

        when(kafkaProducerAgent.produce(kRecords)).thenReturn(Future.successful(kRecords))

        val result = Source.single(kRecords).runWith(KSink.produceSequence(kafkaProducerAgent))

        whenReady(result) { _ =>
          verify(kafkaProducerAgent).produce(kRecords)
        }
      }

    }

    "produce and commit in transaction record on a topic" should {

      val consumerGroup = kafkaConsumerAgent.consumerGroup

      "produce and commit in transaction a single KRecord" in {

        kRecords.foreach { record =>
          when(kafkaProducerAgent.produceAndCommit(record, consumerGroup)).thenReturn(Future.successful(record))
        }

        when(kafkaConsumerAgent.consumerGroup).thenReturn(consumerGroup)

        val result =
          Source.fromIterator(() => kRecords.iterator).runWith(KSink.produceAndCommit(kafkaProducerAgent, kafkaConsumerAgent))

        whenReady(result) { _ =>
          kRecords.foreach { record =>
            verify(kafkaProducerAgent).produceAndCommit(record, consumerGroup)
          }
          verify(kafkaConsumerAgent, times(kRecords.size)).consumerGroup
        }
      }

      "produce and commit in transaction a list of KRecord" in {

        when(kafkaProducerAgent.produceAndCommit(kRecords, consumerGroup)).thenReturn(Future.successful(kRecords))

        when(kafkaConsumerAgent.consumerGroup).thenReturn(consumerGroup)

        val result = Source.single(kRecords).runWith(KSink.produceSequenceAndCommit(kafkaProducerAgent, kafkaConsumerAgent))

        whenReady(result) { _ =>
          verify(kafkaProducerAgent).produceAndCommit(kRecords, consumerGroup)
          verify(kafkaConsumerAgent).consumerGroup
        }
      }

    }

  }

}
