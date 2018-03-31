package com.github.simoexpo.as4k

import akka.stream.scaladsl.Source
import com.github.simoexpo.as4k.consumer.KafkaConsumerAgent
import com.github.simoexpo.as4k.helper.DataHelperSpec
import com.github.simoexpo.as4k.producer.{KafkaSimpleProducerAgent, KafkaTransactionalProducerAgent}
import com.github.simoexpo.{ActorSystemSpec, BaseSpec}
import org.mockito.Mockito._
import org.scalatest.BeforeAndAfterEach
import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}

import scala.concurrent.Future

class KSinkSpec
    extends BaseSpec
    with ScalaFutures
    with ActorSystemSpec
    with IntegrationPatience
    with BeforeAndAfterEach
    with DataHelperSpec {

  private val kafkaConsumerAgent: KafkaConsumerAgent[Int, String] = mock[KafkaConsumerAgent[Int, String]]
  private val kafkaSimpleProducerAgent: KafkaSimpleProducerAgent[Int, String] = mock[KafkaSimpleProducerAgent[Int, String]]
  private val kafkaTransactionalProducerAgent: KafkaTransactionalProducerAgent[Int, String] =
    mock[KafkaTransactionalProducerAgent[Int, String]]

  override def beforeEach(): Unit =
    reset(kafkaConsumerAgent, kafkaSimpleProducerAgent, kafkaTransactionalProducerAgent)

  "KSink" when {

    val topic = "topic"
    val partition = 1

    val kRecords = Range(0, 100).map(n => aKRecord(n, n, s"value$n", topic, partition)).toList

    "producing records on a topic" should {

      "allow to produce single KRecord with a KafkaSimpleProducerAgent" in {

        kRecords.foreach { record =>
          when(kafkaSimpleProducerAgent.produce(record)).thenReturn(Future.successful(record))
        }

        val result = Source.fromIterator(() => kRecords.iterator).runWith(KSink.produce(kafkaSimpleProducerAgent))

        whenReady(result) { _ =>
          kRecords.foreach { record =>
            verify(kafkaSimpleProducerAgent).produce(record)
          }
        }

      }

      "allow to produce a list of KRecord in transaction with a KafkaTransactionalProducerAgent" in {

        when(kafkaTransactionalProducerAgent.produce(kRecords)).thenReturn(Future.successful(kRecords))

        val result = Source.single(kRecords).runWith(KSink.produceSequence(kafkaTransactionalProducerAgent))

        whenReady(result) { _ =>
          verify(kafkaTransactionalProducerAgent).produce(kRecords)
        }
      }

      "fail" in {}

    }

    "producing and committing in transaction record on a topic" should {

      val consumerGroup = kafkaConsumerAgent.consumerGroup

      "allow to produce and commit in transaction a single KRecord with a KafkaTransactionalProducerAgent" in {

        kRecords.foreach { record =>
          when(kafkaTransactionalProducerAgent.produceAndCommit(record, consumerGroup)).thenReturn(Future.successful(record))
        }

        when(kafkaConsumerAgent.consumerGroup).thenReturn(consumerGroup)

        val result =
          Source
            .fromIterator(() => kRecords.iterator)
            .runWith(KSink.produceAndCommit(kafkaTransactionalProducerAgent, kafkaConsumerAgent))

        whenReady(result) { _ =>
          kRecords.foreach { record =>
            verify(kafkaTransactionalProducerAgent).produceAndCommit(record, consumerGroup)
          }
          verify(kafkaConsumerAgent, times(kRecords.size)).consumerGroup
        }
      }

      "allow to produce and commit in transaction a list of KRecord with a KafkaTransactionalProducerAgent" in {

        when(kafkaTransactionalProducerAgent.produceAndCommit(kRecords, consumerGroup)).thenReturn(Future.successful(kRecords))

        when(kafkaConsumerAgent.consumerGroup).thenReturn(consumerGroup)

        val result =
          Source.single(kRecords).runWith(KSink.produceSequenceAndCommit(kafkaTransactionalProducerAgent, kafkaConsumerAgent))

        whenReady(result) { _ =>
          verify(kafkaTransactionalProducerAgent).produceAndCommit(kRecords, consumerGroup)
          verify(kafkaConsumerAgent).consumerGroup
        }
      }

      "fail" in {}

    }

  }

}
