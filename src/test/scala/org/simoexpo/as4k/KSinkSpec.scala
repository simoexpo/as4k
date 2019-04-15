package org.simoexpo.as4k

import akka.stream.scaladsl.Source
import org.mockito.Mockito._
import org.scalatest.BeforeAndAfterEach
import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import org.simoexpo.as4k.consumer.KafkaConsumerAgent
import org.simoexpo.as4k.producer.KafkaProducerActor.KafkaProduceException
import org.simoexpo.as4k.producer.{KafkaSimpleProducerAgent, KafkaTransactionalProducerAgent}
import org.simoexpo.as4k.testing.{ActorSystemSpec, BaseSpec, DataHelperSpec}

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

    val inTopic = "input_topic"
    val outTopic = "output_topic"
    val partitions = 3

    val kRecords = Range(0, 100).map(n => aKRecord(n, n, s"value$n", inTopic, n % partitions, "defaultGroup")).toList

    "producing records on a topic" should {

      "allow to produce single KRecord with a KafkaSimpleProducerAgent" in {

        kRecords.foreach { record =>
          when(kafkaSimpleProducerAgent.produce(record, outTopic)).thenReturn(Future.successful(record))
        }

        val result = Source.fromIterator(() => kRecords.iterator).runWith(KSink.produce(outTopic)(kafkaSimpleProducerAgent))

        whenReady(result) { _ =>
          kRecords.foreach { record =>
            verify(kafkaSimpleProducerAgent).produce(record, outTopic)
          }
        }

      }

      "allow to produce a sequence of KRecord in transaction with a KafkaTransactionalProducerAgent" in {

        when(kafkaTransactionalProducerAgent.produce(kRecords, outTopic)).thenReturn(Future.successful(kRecords))

        val result = Source.single(kRecords).runWith(KSink.produceSequence(outTopic)(kafkaTransactionalProducerAgent))

        whenReady(result) { _ =>
          verify(kafkaTransactionalProducerAgent).produce(kRecords, outTopic)
        }
      }

      "fail with a KafkaProduceException if fail to produce a single record" in {

        when(kafkaSimpleProducerAgent.produce(kRecords.head, outTopic))
          .thenReturn(Future.failed(KafkaProduceException(new RuntimeException("something bad happened!"))))

        val result = Source.fromIterator(() => kRecords.iterator).runWith(KSink.produce(outTopic)(kafkaSimpleProducerAgent))

        whenReady(result.failed) { ex =>
          verify(kafkaSimpleProducerAgent).produce(kRecords.head, outTopic)
          ex shouldBe a[KafkaProduceException]
        }
      }

      "fail with a KafkaProduceException if fail to produce a sequence of record in transaction" in {

        when(kafkaTransactionalProducerAgent.produce(kRecords, outTopic))
          .thenReturn(Future.failed(KafkaProduceException(new RuntimeException("something bad happened!"))))

        val result = Source.single(kRecords).runWith(KSink.produceSequence(outTopic)(kafkaTransactionalProducerAgent))

        whenReady(result.failed) { ex =>
          verify(kafkaTransactionalProducerAgent).produce(kRecords, outTopic)
          ex shouldBe a[KafkaProduceException]
        }
      }

    }

    "producing and committing in transaction record on a topic" should {

      val consumerGroup = kafkaConsumerAgent.consumerGroup

      "allow to produce and commit in transaction a single KRecord with a KafkaTransactionalProducerAgent" in {

        kRecords.foreach { record =>
          when(kafkaTransactionalProducerAgent.produceAndCommit(record, outTopic)).thenReturn(Future.successful(record))
        }

        when(kafkaConsumerAgent.consumerGroup).thenReturn(consumerGroup)

        val result =
          Source.fromIterator(() => kRecords.iterator).runWith(KSink.produceAndCommit(outTopic)(kafkaTransactionalProducerAgent))

        whenReady(result) { _ =>
          kRecords.foreach { record =>
            verify(kafkaTransactionalProducerAgent).produceAndCommit(record, outTopic)
          }
        }
      }

      "allow to produce and commit in transaction a list of KRecord with a KafkaTransactionalProducerAgent" in {

        when(kafkaTransactionalProducerAgent.produceAndCommit(kRecords, outTopic)).thenReturn(Future.successful(kRecords))

        when(kafkaConsumerAgent.consumerGroup).thenReturn(consumerGroup)

        val result =
          Source.single(kRecords).runWith(KSink.produceSequenceAndCommit(outTopic)(kafkaTransactionalProducerAgent))

        whenReady(result) { _ =>
          verify(kafkaTransactionalProducerAgent).produceAndCommit(kRecords, outTopic)
        }
      }

      "fail with a KafkaProduceException if fail to produce and commit a single record" in {

        when(kafkaTransactionalProducerAgent.produceAndCommit(kRecords.head, outTopic))
          .thenReturn(Future.failed(KafkaProduceException(new RuntimeException("something bad happened!"))))

        when(kafkaConsumerAgent.consumerGroup).thenReturn(consumerGroup)

        val result =
          Source.fromIterator(() => kRecords.iterator).runWith(KSink.produceAndCommit(outTopic)(kafkaTransactionalProducerAgent))

        whenReady(result.failed) { ex =>
          verify(kafkaTransactionalProducerAgent).produceAndCommit(kRecords.head, outTopic)
          ex shouldBe a[KafkaProduceException]
        }
      }

      "fail with a KafkaProduceException if fail to produce and commit a sequence of record in transaction" in {

        when(kafkaTransactionalProducerAgent.produceAndCommit(kRecords, outTopic))
          .thenReturn(Future.failed(KafkaProduceException(new RuntimeException("something bad happened!"))))

        when(kafkaConsumerAgent.consumerGroup).thenReturn(consumerGroup)

        val result =
          Source.single(kRecords).runWith(KSink.produceSequenceAndCommit(outTopic)(kafkaTransactionalProducerAgent))

        whenReady(result.failed) { ex =>
          verify(kafkaTransactionalProducerAgent).produceAndCommit(kRecords, outTopic)
          ex shouldBe a[KafkaProduceException]
        }
      }

    }

  }

}
