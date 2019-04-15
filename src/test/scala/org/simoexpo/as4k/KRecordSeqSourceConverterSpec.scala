package org.simoexpo.as4k

import akka.stream.scaladsl.{Sink, Source}
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.mockito.Mockito.{reset, verify, when}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import org.simoexpo.as4k.KSource._
import org.simoexpo.as4k.consumer.KafkaConsumerActor.KafkaCommitException
import org.simoexpo.as4k.consumer.KafkaConsumerAgent
import org.simoexpo.as4k.producer.KafkaProducerActor.KafkaProduceException
import org.simoexpo.as4k.producer.{KafkaSimpleProducerAgent, KafkaTransactionalProducerAgent}
import org.simoexpo.as4k.testing.{ActorSystemSpec, BaseSpec, DataHelperSpec}

import scala.concurrent.Future

class KRecordSeqSourceConverterSpec
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

  "KRecordSeqSourceConverter" when {

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

      "call commit on KafkaConsumerAgent for a list of KRecord" in {

        when(kafkaConsumerAgent.commitBatch(records, Some(callback))).thenReturn(Future.successful(records))

        val recordConsumed =
          Source.single(records).commit()(kafkaConsumerAgent, Some(callback)).mapConcat(_.toList).runWith(Sink.seq)

        whenReady(recordConsumed) { _ =>
          verify(kafkaConsumerAgent).commitBatch(records, Some(callback))
        }
      }

      "fail if kafka consumer fail to commit for a list of KRecord" in {

        when(kafkaConsumerAgent.commitBatch(records, Some(callback)))
          .thenReturn(Future.failed(KafkaCommitException(new RuntimeException("Something bad happened!"))))

        val recordConsumed =
          Source.single(records).commit()(kafkaConsumerAgent, Some(callback)).mapConcat(_.toList).runWith(Sink.seq)

        whenReady(recordConsumed.failed) { exception =>
          verify(kafkaConsumerAgent).commitBatch(records, Some(callback))

          exception shouldBe a[KafkaCommitException]
        }
      }
    }

    "producing KRecords on a topic" should {

      "allow to call produce on KafkaTransactionalProducerAgent for a list of KRecord" in {

        when(kafkaTransactionalProducerAgent.produce(records, outTopic)).thenReturn(Future.successful(records))

        val recordProduced =
          Source.single(records).produce(outTopic)(kafkaTransactionalProducerAgent).runWith(Sink.seq)

        whenReady(recordProduced) { _ =>
          verify(kafkaTransactionalProducerAgent).produce(records, outTopic)
        }
      }

      "fail with a KafkaProduceException if KafkaTransactionalProducerAgent fail to produce a list of KRecord" in {

        when(kafkaTransactionalProducerAgent.produce(records, outTopic))
          .thenReturn(Future.failed(KafkaProduceException(new RuntimeException("Something bad happened!"))))

        val recordProduced =
          Source.single(records).produce(outTopic)(kafkaTransactionalProducerAgent).runWith(Sink.seq)

        whenReady(recordProduced.failed) { exception =>
          verify(kafkaTransactionalProducerAgent).produce(records, outTopic)

          exception shouldBe a[KafkaProduceException]
        }
      }
    }

    "producing and committing KRecord on a topic" should {

      "allow to call produceAndCommit on KafkaTransactionalProducerAgent for a list of KRecord" in {

        when(kafkaTransactionalProducerAgent.produceAndCommit(records, outTopic)).thenReturn(Future.successful(records))

        val recordProduced =
          Source.single(records).produceAndCommit(outTopic)(kafkaTransactionalProducerAgent).runWith(Sink.seq)

        whenReady(recordProduced) { _ =>
          verify(kafkaTransactionalProducerAgent).produceAndCommit(records, outTopic)
        }
      }

      "fail with a KafkaProduceException if KafkaTransactionalProducerAgent fail to produce and commit in transaction a list of KRecord" in {

        when(kafkaTransactionalProducerAgent.produceAndCommit(records, outTopic))
          .thenReturn(Future.failed(KafkaProduceException(new RuntimeException("Something bad happened!"))))

        val recordProduced =
          Source.single(records).produceAndCommit(outTopic)(kafkaTransactionalProducerAgent).runWith(Sink.seq)

        whenReady(recordProduced.failed) { exception =>
          verify(kafkaTransactionalProducerAgent).produceAndCommit(records, outTopic)

          exception shouldBe a[KafkaProduceException]
        }
      }
    }
  }
}
