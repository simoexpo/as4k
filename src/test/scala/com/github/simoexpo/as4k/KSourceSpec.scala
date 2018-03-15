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
import com.github.simoexpo.as4k.consumer.KafkaConsumerActor.{ConsumerToken, KafkaCommitException, KafkaPollingException}
import com.github.simoexpo.as4k.consumer.KafkaConsumerAgent
import com.github.simoexpo.as4k.factory.{CallbackFactory, KRecord}
import org.mockito.invocation.InvocationOnMock

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

  "KSource" when {

    val records1 = Range(0, 100).map(n => aKRecord(n, n, s"value$n", topic, partition)).toList
    val records2 = Range(100, 200).map(n => aKRecord(n, n, s"value$n", topic, partition)).toList
    val records3 = Range(200, 220).map(n => aKRecord(n, n, s"value$n", topic, partition)).toList

    "producing a source from kafka records" should {

      "ask kafka consumer for new records" in {

        val totalRecordsSize = Seq(records1, records2, records3).map(_.size).sum
        val expectedRecords = Seq(records1, records2, records3).flatten

        when(kafkaConsumerAgent.askForRecords(ConsumerToken))
          .thenReturn(Future.successful(records1))
          .thenReturn(Future.successful(records2))
          .thenReturn(Future.successful(records3))

        val recordsConsumed = KSource.fromKafkaConsumer(kafkaConsumerAgent).take(totalRecordsSize).runWith(Sink.seq)

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

        when(kafkaConsumerAgent.askForRecords(ConsumerToken))
          .thenReturn(Future.failed(KafkaPollingException(new RuntimeException("Something bad happened!"))))
          .thenReturn(Future.successful(records1))
          .thenReturn(Future.successful(records2))

        val recordsConsumed = KSource.fromKafkaConsumer(kafkaConsumerAgent).take(totalRecordsSize).runWith(Sink.seq)

        whenReady(recordsConsumed) { records =>
          records.size shouldBe totalRecordsSize
          records.toList shouldBe expectedRecords

          verify(kafkaConsumerAgent, invokedAtLeast(3)).askForRecords(ConsumerToken)

        }
      }
    }

    "committing synchronously records" should {

      "call commit on KafkaConsumerAgent for a single ConsumerRecord" in {

        val totalRecordsSize = records1.size
        val expectedRecords = records1

        when(kafkaConsumerAgent.askForRecords(ConsumerToken)).thenReturn(Future.successful(records1))
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

      "call commit on KafkaConsumerAgent for a list of ConsumerRecord" in {

        val totalRecordsSize = records1.size
        val groupSize = 10
        val expectedRecords = records1

        when(kafkaConsumerAgent.askForRecords(ConsumerToken)).thenReturn(Future.successful(records1))
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

      "fail if kafka consumer fail to commit for a single ConsumerRecord" in {

        val totalRecordsSize = records1.size

        when(kafkaConsumerAgent.askForRecords(ConsumerToken)).thenReturn(Future.successful(records1))
        when(kafkaConsumerAgent.commit(any[KRecord[Int, String]]))
          .thenReturn(Future.failed(KafkaCommitException(new RuntimeException("Something bad happened!"))))

        val recordConsumed =
          KSource.fromKafkaConsumer(kafkaConsumerAgent).take(totalRecordsSize).commit(kafkaConsumerAgent).runWith(Sink.seq)

        whenReady(recordConsumed.failed) { exception =>
          verify(kafkaConsumerAgent, atLeastOnce()).askForRecords(ConsumerToken)
          exception shouldBe a[KafkaCommitException]
        }
      }

      "fail if kafka consumer fail to commit for a list of ConsumerRecord" in {

        val totalRecordsSize = records1.size
        val groupSize = 10

        when(kafkaConsumerAgent.askForRecords(ConsumerToken)).thenReturn(Future.successful(records1))
        when(kafkaConsumerAgent.commit(any[Seq[KRecord[Int, String]]]))
          .thenReturn(Future.failed(KafkaCommitException(new RuntimeException("Something bad happened!"))))

        val recordConsumed =
          KSource
            .fromKafkaConsumer(kafkaConsumerAgent)
            .take(totalRecordsSize)
            .grouped(groupSize)
            .commit(kafkaConsumerAgent)
            .mapConcat(_.toList)
            .runWith(Sink.seq)

        whenReady(recordConsumed.failed) { exception =>
          verify(kafkaConsumerAgent, atLeastOnce()).askForRecords(ConsumerToken)
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

      "call commit on KafkaConsumerAgent for a single ConsumerRecord" in {

        val totalRecordsSize = records1.size
        val expectedRecords = records1

        when(kafkaConsumerAgent.askForRecords(ConsumerToken)).thenReturn(Future.successful(records1))
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

      "call commit on KafkaConsumerAgent for a list of ConsumerRecord" in {

        val totalRecordsSize = records1.size
        val groupSize = 10
        val expectedRecords = records1

        when(kafkaConsumerAgent.askForRecords(ConsumerToken)).thenReturn(Future.successful(records1))
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

      "fail if kafka consumer fail to commit for a single ConsumerRecord" in {

        val totalRecordsSize = records1.size

        when(kafkaConsumerAgent.askForRecords(ConsumerToken)).thenReturn(Future.successful(records1))
        when(kafkaConsumerAgent.commitAsync(any[KRecord[Int, String]], mockitoEq(callback)))
          .thenReturn(Future.failed(KafkaCommitException(new RuntimeException("Something bad happened!"))))

        val recordConsumed =
          KSource
            .fromKafkaConsumer(kafkaConsumerAgent)
            .take(totalRecordsSize)
            .commitAsync(kafkaConsumerAgent, callback)
            .runWith(Sink.seq)

        whenReady(recordConsumed.failed) { exception =>
          verify(kafkaConsumerAgent, atLeastOnce()).askForRecords(ConsumerToken)
          exception shouldBe a[KafkaCommitException]
        }
      }

      "fail if kafka consumer fail to commit for a list of ConsumerRecord" in {

        val totalRecordsSize = records1.size
        val groupSize = 10

        when(kafkaConsumerAgent.askForRecords(ConsumerToken)).thenReturn(Future.successful(records1))
        when(kafkaConsumerAgent.commitAsync(any[Seq[KRecord[Int, String]]], mockitoEq(callback)))
          .thenReturn(Future.failed(KafkaCommitException(new RuntimeException("Something bad happened!"))))

        val recordConsumed =
          KSource
            .fromKafkaConsumer(kafkaConsumerAgent)
            .take(totalRecordsSize)
            .grouped(groupSize)
            .commitAsync(kafkaConsumerAgent, callback)
            .mapConcat(_.toList)
            .runWith(Sink.seq)

        whenReady(recordConsumed.failed) { exception =>
          verify(kafkaConsumerAgent, atLeastOnce()).askForRecords(ConsumerToken)
          exception shouldBe a[KafkaCommitException]
        }
      }
    }
  }

  private def aKRecord[K, V](offset: Long, key: K, value: V, topic: String, partition: Int) =
    KRecord(key, value, topic, partition, offset, System.currentTimeMillis())
}
