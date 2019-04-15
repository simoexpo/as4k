package org.simoexpo.as4k

import akka.stream.scaladsl.{Sink, Source}
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.mockito.ArgumentMatchers.{any, eq => mockitoEq}
import org.mockito.Mockito._
import org.scalatest.BeforeAndAfterEach
import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import org.simoexpo.as4k.KSource._
import org.simoexpo.as4k.consumer.KafkaConsumerActor.KafkaCommitException
import org.simoexpo.as4k.consumer.KafkaConsumerAgent
import org.simoexpo.as4k.model.KRecord
import org.simoexpo.as4k.producer.KafkaProducerActor.KafkaProduceException
import org.simoexpo.as4k.producer.{KafkaSimpleProducerAgent, KafkaTransactionalProducerAgent}
import org.simoexpo.as4k.testing.{ActorSystemSpec, BaseSpec, DataHelperSpec}

import scala.concurrent.Future

class KRecordSourceConverterSpec
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

  "KRecordSourceConverter" when {

    val inTopic = "input_topic"
    val outTopic = "output_topic"
    val partitions = 3

    val records = Range(0, 100).map(n => aKRecord(n, n, s"value$n", inTopic, n % partitions, "defaultGroup")).toList

    "committing records" should {

      val callback = (offsets: Map[TopicPartition, OffsetAndMetadata], exception: Option[Exception]) =>
        exception match {
          case None    => println(s"successfully commit offset $offsets")
          case Some(_) => println(s"fail commit offset $offsets")
      }

      "call commit on KafkaConsumerAgent for a single KRecord" in {

        records.foreach { record =>
          when(kafkaConsumerAgent.commit(record, Some(callback))).thenReturn(Future.successful(record))
        }

        val recordConsumed =
          Source.fromIterator(() => records.iterator).commit()(kafkaConsumerAgent, Some(callback)).runWith(Sink.seq)

        whenReady(recordConsumed) { _ =>
          records.foreach { record =>
            verify(kafkaConsumerAgent).commit(record, Some(callback))
          }
        }
      }

      "fail if kafka consumer fail to commit for a single KRecord" in {

        when(kafkaConsumerAgent.commit(any[KRecord[Int, String]], mockitoEq(Some(callback))))
          .thenReturn(Future.failed(KafkaCommitException(new RuntimeException("Something bad happened!"))))

        val recordConsumed =
          Source.fromIterator(() => records.iterator).commit()(kafkaConsumerAgent, Some(callback)).runWith(Sink.seq)

        whenReady(recordConsumed.failed) { exception =>
          verify(kafkaConsumerAgent).commit(records.head, Some(callback))

          exception shouldBe a[KafkaCommitException]
        }
      }
    }

    "mapping the value of KRecord" should {

      "return a Source of KRecord with mapped value" in {

        val recordMapped =
          Source.fromIterator(() => records.iterator).mapValue(_.toUpperCase).runWith(Sink.seq)

        whenReady(recordMapped) { records =>
          records shouldBe records.map(_.mapValue(_.toUpperCase))
        }
      }

    }

    "producing KRecords on a topic" should {

      "allow to call produce on KafkaSimpleProducerAgent for a single KRecord" in {

        records.foreach { record =>
          when(kafkaSimpleProducerAgent.produce(record, outTopic)).thenReturn(Future.successful(record))
        }

        val recordProduced =
          Source.fromIterator(() => records.iterator).produce()(outTopic)(kafkaSimpleProducerAgent).runWith(Sink.seq)

        whenReady(recordProduced) { _ =>
          records.foreach { record =>
            verify(kafkaSimpleProducerAgent).produce(record, outTopic)
          }
        }

      }

      "fail with a KafkaProduceException if KafkaSimpleProducerAgent fail to produce a single KRecord" in {

        val failedIndex = 20

        records.slice(0, failedIndex).foreach { record =>
          when(kafkaSimpleProducerAgent.produce(record, outTopic)).thenReturn(Future.successful(record))
        }

        when(kafkaSimpleProducerAgent.produce(records(failedIndex), outTopic))
          .thenReturn(Future.failed(KafkaProduceException(new RuntimeException("Something bad happened!"))))

        val recordProduced =
          Source.fromIterator(() => records.iterator).produce()(outTopic)(kafkaSimpleProducerAgent).runWith(Sink.seq)

        whenReady(recordProduced.failed) { exception =>
          records.slice(0, failedIndex).foreach { record =>
            verify(kafkaSimpleProducerAgent).produce(record, outTopic)
          }
          verify(kafkaSimpleProducerAgent).produce(records(failedIndex), outTopic)

          exception shouldBe a[KafkaProduceException]
        }
      }
    }

    "producing and committing KRecord on a topic" should {

      "allow to call produceAndCommit on KafkaTransactionalProducerAgent for a single KRecord" in {

        records.foreach { record =>
          when(kafkaTransactionalProducerAgent.produceAndCommit(record, outTopic)).thenReturn(Future.successful(record))
        }

        val recordProduced =
          Source
            .fromIterator(() => records.iterator)
            .produceAndCommit(outTopic)(kafkaTransactionalProducerAgent)
            .runWith(Sink.seq)

        whenReady(recordProduced) { _ =>
          records.foreach { record =>
            verify(kafkaTransactionalProducerAgent).produceAndCommit(record, outTopic)
          }
        }
      }

      "fail with a KafkaProduceException if KafkaTransactionalProducerAgent fail to produce and commit in transaction a single KRecord" in {

        val failedIndex = 20

        records.slice(0, failedIndex).foreach { record =>
          when(kafkaTransactionalProducerAgent.produceAndCommit(record, outTopic)).thenReturn(Future.successful(record))
        }

        when(kafkaTransactionalProducerAgent.produceAndCommit(records(failedIndex), outTopic))
          .thenReturn(Future.failed(KafkaProduceException(new RuntimeException("Something bad happened!"))))

        val recordProduced =
          Source
            .fromIterator(() => records.iterator)
            .produceAndCommit(outTopic)(kafkaTransactionalProducerAgent)
            .runWith(Sink.seq)

        whenReady(recordProduced.failed) { exception =>
          records.slice(0, failedIndex).foreach { record =>
            verify(kafkaTransactionalProducerAgent).produceAndCommit(record, outTopic)
          }
          verify(kafkaTransactionalProducerAgent).produceAndCommit(records(failedIndex), outTopic)

          exception shouldBe a[KafkaProduceException]
        }
      }
    }
  }
}
