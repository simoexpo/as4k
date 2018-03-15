package com.github.simoexpo.as4k

import akka.stream.scaladsl.{Sink, Source}
import com.github.simoexpo.{ActorSystemSpec, BaseSpec}
import com.github.simoexpo.as4k.KSource._
import com.github.simoexpo.as4k.consumer.KafkaConsumerActor.{ConsumerToken, KafkaCommitException, KafkaPollingException}
import com.github.simoexpo.as4k.consumer.KafkaConsumerAgent
import com.github.simoexpo.as4k.factory.{CallbackFactory, KRecord}
import com.github.simoexpo.as4k.producer.KafkaProducerActor.KafkaProduceException
import com.github.simoexpo.as4k.producer.KafkaProducerAgent
import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.TopicPartition
import org.mockito.ArgumentMatchers.{any, eq => mockitoEq}
import org.mockito.Mockito.{atLeast => invokedAtLeast, _}
import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import org.scalatest.BeforeAndAfterEach

import scala.concurrent.Future
import scala.concurrent.duration._

class KSourceSpec
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

  "KSource" when {

    val topic = "topic"
    val partition = 1

    val records1 = Range(0, 100).map(n => aKRecord(n, n, s"value$n", topic, partition)).toList
    val records2 = Range(100, 200).map(n => aKRecord(n, n, s"value$n", topic, partition)).toList
    val records3 = Range(200, 220).map(n => aKRecord(n, n, s"value$n", topic, partition)).toList

    "producing a source of KRecord" should {

      "ask kafka consumer for new records" in {

        val totalRecordsSize = Seq(records1, records2, records3).map(_.size).sum
        val expectedRecords = Seq(records1, records2, records3).flatten

        val minBackoff = Some(1.second)
        val maxBackoff = Some(1.second)
        val randomFactor = Some(0D)

        when(kafkaConsumerAgent.askForRecords(ConsumerToken))
          .thenReturn(Future.successful(records1))
          .thenReturn(Future.successful(records2))
          .thenReturn(Future.successful(records3))

        val recordsConsumed =
          KSource.fromKafkaConsumer(kafkaConsumerAgent).take(totalRecordsSize).runWith(Sink.seq)

        whenReady(recordsConsumed) { records =>
          records.size shouldBe totalRecordsSize
          records.toList shouldBe expectedRecords

          verify(kafkaConsumerAgent, invokedAtLeast(3)).askForRecords(ConsumerToken)

        }
      }

      "not end when not receiving new records from kafka consumer agent" in {

        val totalRecordsSize = Seq(records1, records2).map(_.size).sum
        val expectedRecords = Seq(records1, records2).flatten

        val emptyRecords = List.empty[KRecord[Int, String]]

        when(kafkaConsumerAgent.askForRecords(ConsumerToken))
          .thenReturn(Future.successful(records1))
          .thenReturn(Future.successful(emptyRecords))
          .thenReturn(Future.successful(records2))

        val recordsConsumed = KSource.fromKafkaConsumer(kafkaConsumerAgent).take(totalRecordsSize).runWith(Sink.seq)

        whenReady(recordsConsumed) { records =>
          records.size shouldBe totalRecordsSize
          records.toList shouldBe expectedRecords

          verify(kafkaConsumerAgent, invokedAtLeast(3)).askForRecords(ConsumerToken)

        }
      }

      "retry to retrieve records if the kafka consumer agent fails" in {

        val totalRecordsSize = Seq(records1, records2).map(_.size).sum
        val expectedRecords = Seq(records1, records2).flatten

        val minBackoff = Some(0.second)
        val randomFactor = Some(0D)

        when(kafkaConsumerAgent.askForRecords(ConsumerToken))
          .thenReturn(Future.failed(KafkaPollingException(new RuntimeException("Something bad happened!"))))
          .thenReturn(Future.successful(records1))
          .thenReturn(Future.successful(records2))

        val recordsConsumed =
          KSource
            .fromKafkaConsumer(kafkaConsumerAgent, minBackoff = minBackoff, randomFactor = randomFactor)
            .take(totalRecordsSize)
            .runWith(Sink.seq)

        whenReady(recordsConsumed) { records =>
          records.size shouldBe totalRecordsSize
          records.toList shouldBe expectedRecords

          verify(kafkaConsumerAgent, invokedAtLeast(3)).askForRecords(ConsumerToken)

        }
      }
    }

    "committing synchronously records" should {

      "call commit on KafkaConsumerAgent for a single KRecord" in {

        records1.foreach { record =>
          when(kafkaConsumerAgent.commit(record)).thenReturn(Future.successful(record))
        }

        val recordConsumed =
          Source.fromIterator(() => records1.iterator).commit(kafkaConsumerAgent).runWith(Sink.seq)

        whenReady(recordConsumed) { _ =>
          records1.foreach { record =>
            verify(kafkaConsumerAgent).commit(record)
          }
        }
      }

      "call commit on KafkaConsumerAgent for a list of KRecord" in {

        when(kafkaConsumerAgent.commit(records1)).thenReturn(Future.successful(records1))

        val recordConsumed =
          Source.single(records1).commit(kafkaConsumerAgent).mapConcat(_.toList).runWith(Sink.seq)

        whenReady(recordConsumed) { _ =>
          verify(kafkaConsumerAgent).commit(records1)
        }
      }

      "fail if kafka consumer fail to commit for a single KRecord" in {

        records1.foreach { record =>
          when(kafkaConsumerAgent.commit(record))
            .thenReturn(Future.failed(KafkaCommitException(new RuntimeException("Something bad happened!"))))
        }

        val recordConsumed =
          Source.fromIterator(() => records1.iterator).commit(kafkaConsumerAgent).runWith(Sink.seq)

        whenReady(recordConsumed.failed) { exception =>
          verify(kafkaConsumerAgent).commit(records1.head)

          exception shouldBe a[KafkaCommitException]
        }
      }

      "fail if kafka consumer fail to commit for a list of KRecord" in {

        when(kafkaConsumerAgent.commit(records1))
          .thenReturn(Future.failed(KafkaCommitException(new RuntimeException("Something bad happened!"))))

        val recordConsumed =
          Source.single(records1).commit(kafkaConsumerAgent).mapConcat(_.toList).runWith(Sink.seq)

        whenReady(recordConsumed.failed) { exception =>
          verify(kafkaConsumerAgent).commit(records1)

          exception shouldBe a[KafkaCommitException]
        }
      }
    }

    "committing asynchronously records" should {

      val callback = CallbackFactory { (offset: Map[TopicPartition, OffsetAndMetadata], exception: Option[Exception]) =>
        exception match {
          case None     => println(s"successfully commit offset $offset")
          case Some(ex) => throw ex
        }
      }

      "call commit on KafkaConsumerAgent for a single KRecord" in {

        records1.foreach { record =>
          when(kafkaConsumerAgent.commitAsync(record, callback)).thenReturn(Future.successful(record))
        }

        val recordConsumed =
          Source.fromIterator(() => records1.iterator).commitAsync(kafkaConsumerAgent, callback).runWith(Sink.seq)

        whenReady(recordConsumed) { _ =>
          records1.foreach { record =>
            verify(kafkaConsumerAgent).commitAsync(record, callback)
          }
        }
      }

      "call commit on KafkaConsumerAgent for a list of KRecord" in {

        when(kafkaConsumerAgent.commitAsync(records1, callback)).thenReturn(Future.successful(records1))

        val recordConsumed =
          Source.single(records1).commitAsync(kafkaConsumerAgent, callback).mapConcat(_.toList).runWith(Sink.seq)

        whenReady(recordConsumed) { _ =>
          verify(kafkaConsumerAgent).commitAsync(records1, callback)
        }
      }

      "fail if kafka consumer fail to commit for a single KRecord" in {

        when(kafkaConsumerAgent.commitAsync(any[KRecord[Int, String]], mockitoEq(callback)))
          .thenReturn(Future.failed(KafkaCommitException(new RuntimeException("Something bad happened!"))))

        val recordConsumed =
          Source.fromIterator(() => records1.iterator).commitAsync(kafkaConsumerAgent, callback).runWith(Sink.seq)

        whenReady(recordConsumed.failed) { exception =>
          verify(kafkaConsumerAgent).commitAsync(records1.head, callback)

          exception shouldBe a[KafkaCommitException]
        }
      }

      "fail if kafka consumer fail to commit for a list of KRecord" in {

        when(kafkaConsumerAgent.commitAsync(records1, callback))
          .thenReturn(Future.failed(KafkaCommitException(new RuntimeException("Something bad happened!"))))

        val recordConsumed =
          Source.single(records1).commitAsync(kafkaConsumerAgent, callback).mapConcat(_.toList).runWith(Sink.seq)

        whenReady(recordConsumed.failed) { exception =>
          verify(kafkaConsumerAgent).commitAsync(records1, callback)

          exception shouldBe a[KafkaCommitException]
        }
      }
    }

    "producing KRecords on a topic" should {

      "call produce on KafkaProducerAgent for a single KRecord" in {

        records1.foreach { record =>
          when(kafkaProducerAgent.produce(record)).thenReturn(Future.successful(record))
        }

        val recordProduced =
          Source.fromIterator(() => records1.iterator).produce(kafkaProducerAgent).runWith(Sink.seq)

        whenReady(recordProduced) { _ =>
          records1.foreach { record =>
            verify(kafkaProducerAgent).produce(record)
          }
        }

      }

      "call produce on KafkaProducerAgent for a list of KRecord" in {

        when(kafkaProducerAgent.produce(records1)).thenReturn(Future.successful(records1))

        val recordProduced =
          Source.single(records1).produce(kafkaProducerAgent).runWith(Sink.seq)

        whenReady(recordProduced) { _ =>
          verify(kafkaProducerAgent).produce(records1)
        }
      }

      "fail with a KafkaProduceException if kafka producer fail to produce a single KRecord" in {

        val failedIndex = 20

        records1.slice(0, failedIndex).foreach { record =>
          when(kafkaProducerAgent.produce(record)).thenReturn(Future.successful(record))
        }

        when(kafkaProducerAgent.produce(records1(failedIndex)))
          .thenReturn(Future.failed(KafkaProduceException(new RuntimeException("Something bad happened!"))))

        val recordProduced =
          Source.fromIterator(() => records1.iterator).produce(kafkaProducerAgent).runWith(Sink.seq)

        whenReady(recordProduced.failed) { exception =>
          records1.slice(0, failedIndex).foreach { record =>
            verify(kafkaProducerAgent).produce(record)
          }
          verify(kafkaProducerAgent).produce(records1(failedIndex))

          exception shouldBe a[KafkaProduceException]
        }
      }

      "fail if kafka producer fail to produce a list of KRecord" in {

        when(kafkaProducerAgent.produce(records1))
          .thenReturn(Future.failed(KafkaProduceException(new RuntimeException("Something bad happened!"))))

        val recordProduced =
          Source.single(records1).produce(kafkaProducerAgent).runWith(Sink.seq)

        whenReady(recordProduced.failed) { exception =>
          verify(kafkaProducerAgent).produce(records1)

          exception shouldBe a[KafkaProduceException]
        }
      }
    }

    "producing and committing KRecord on a topic" should {

      val consumerGroup = kafkaConsumerAgent.consumerGroup

      "call produceAndCommit on KafkaProducerAgent for a single KRecord" in {

        records1.foreach { record =>
          when(kafkaProducerAgent.produceAndCommit(record, consumerGroup)).thenReturn(Future.successful(record))
        }

        val recordProduced =
          Source.fromIterator(() => records1.iterator).produceAndCommit(kafkaProducerAgent, kafkaConsumerAgent).runWith(Sink.seq)

        whenReady(recordProduced) { _ =>
          records1.foreach { record =>
            verify(kafkaProducerAgent).produceAndCommit(record, consumerGroup)
          }
        }
      }

      "call produceAndCommit on KafkaProducerAgent for a list of KRecord" in {

        when(kafkaProducerAgent.produceAndCommit(records1, consumerGroup)).thenReturn(Future.successful(records1))

        val recordProduced =
          Source.single(records1).produceAndCommit(kafkaProducerAgent, kafkaConsumerAgent).runWith(Sink.seq)

        whenReady(recordProduced) { _ =>
          verify(kafkaProducerAgent).produceAndCommit(records1, consumerGroup)
        }
      }

      "fail if kafka producer fail to produce and commit in transaction a single KRecord" in {

        val failedIndex = 20

        records1.slice(0, failedIndex).foreach { record =>
          when(kafkaProducerAgent.produceAndCommit(record, consumerGroup)).thenReturn(Future.successful(record))
        }

        when(kafkaProducerAgent.produceAndCommit(records1(failedIndex), consumerGroup))
          .thenReturn(Future.failed(KafkaProduceException(new RuntimeException("Something bad happened!"))))

        val recordProduced =
          Source.fromIterator(() => records1.iterator).produceAndCommit(kafkaProducerAgent, kafkaConsumerAgent).runWith(Sink.seq)

        whenReady(recordProduced.failed) { exception =>
          records1.slice(0, failedIndex).foreach { record =>
            verify(kafkaProducerAgent).produceAndCommit(record, consumerGroup)
          }
          verify(kafkaProducerAgent).produceAndCommit(records1(failedIndex), consumerGroup)

          exception shouldBe a[KafkaProduceException]
        }
      }

      "fail if kafka producer fail to produce and commit in transaction a list of KRecord" in {

        when(kafkaProducerAgent.produceAndCommit(records1, consumerGroup))
          .thenReturn(Future.failed(KafkaProduceException(new RuntimeException("Something bad happened!"))))

        val recordProduced =
          Source.single(records1).produceAndCommit(kafkaProducerAgent, kafkaConsumerAgent).runWith(Sink.seq)

        whenReady(recordProduced.failed) { exception =>
          verify(kafkaProducerAgent).produceAndCommit(records1, consumerGroup)

          exception shouldBe a[KafkaProduceException]
        }
      }
    }
  }

}
