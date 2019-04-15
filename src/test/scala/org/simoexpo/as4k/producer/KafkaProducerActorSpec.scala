package org.simoexpo.as4k.producer

import akka.actor.Props
import akka.pattern.ask
import akka.testkit.EventFilter
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.TopicPartition
import org.mockito.ArgumentMatcher
import org.mockito.ArgumentMatchers.{any, argThat, isNull}
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.BeforeAndAfterEach
import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import org.simoexpo.as4k.model.KRecord
import org.simoexpo.as4k.producer.KafkaProducerActor.{
  KafkaProduceException,
  ProduceRecord,
  ProduceRecords,
  ProduceRecordsAndCommit
}
import org.simoexpo.as4k.testing.{ActorSystemSpec, BaseSpec, DataHelperSpec}

import scala.language.postfixOps

class KafkaProducerActorSpec
    extends BaseSpec
    with ScalaFutures
    with ActorSystemSpec
    with IntegrationPatience
    with BeforeAndAfterEach
    with DataHelperSpec {

  private val kafkaProducerOption: KafkaProducerOption[Int, String] = mock[KafkaProducerOption[Int, String]]
  private val kafkaProducer: KafkaProducer[Int, String] = mock[KafkaProducer[Int, String]]

  when(kafkaProducerOption.createOne()).thenReturn(kafkaProducer)

  private val kafkaProducerActor = system.actorOf(Props(new KafkaProducerActor(kafkaProducerOption)))

  override def beforeEach(): Unit =
    reset(kafkaProducer, kafkaProducerOption)

  "KafkaProducerActor" when {

    val inTopic = "input_topic"
    val outTopic = "output_topic"
    val partitions = 3

    val kRecords = Range(0, 100).map(n => aKRecord(n, n, s"value$n", inTopic, n % partitions, "defaultGroup")).toList

    "producing records" should {

      "send a single record to the producer" in {

        when(kafkaProducerOption.isTransactional).thenReturn(false)

        doAnswer(new Answer[java.util.concurrent.Future[RecordMetadata]]() {
          override def answer(invocation: InvocationOnMock) = {
            import java.util.concurrent.Executors
            Executors.newSingleThreadExecutor.submit(() => {
              Thread.sleep(10)
              val recordMetadata =
                new RecordMetadata(new TopicPartition("output_topic", 1), 1L, 1L, 1L, 1L.asInstanceOf[java.lang.Long], 1, 1)
              invocation.getArgument[Callback](1).onCompletion(recordMetadata, null)
              recordMetadata
            })
          }
        }).when(kafkaProducer).send(any[ProducerRecord[Int, String]], any[Callback])

        whenReady(kafkaProducerActor ? ProduceRecord(kRecords.head, outTopic)) { _ =>
          verify(kafkaProducer).send(anyProducerRecordWith(kRecords.head), any[Callback])
        }
      }

      "send a sequence of records to the producer in transaction" in {

        when(kafkaProducerOption.isTransactional).thenReturn(true)

        whenReady(kafkaProducerActor ? ProduceRecords(kRecords, outTopic)) { _ =>
          verify(kafkaProducer).beginTransaction()
          kRecords.foreach { record =>
            verify(kafkaProducer).send(anyProducerRecordWith(record))
          }
          verify(kafkaProducer).commitTransaction()
        }
      }

      "fail with a KafkaProduceException if the producer fail to send a single record immediately" in {

        when(kafkaProducer.send(any[ProducerRecord[Int, String]], any[Callback]))
          .thenThrow(new RuntimeException("something bad happened!"))

        whenReady(kafkaProducerActor ? ProduceRecord(kRecords.head, outTopic) failed) { exception =>
          exception shouldBe a[KafkaProduceException]
        }
      }

      "fail with a KafkaProduceException if the producer fail to send a single record during callback" in {

        doAnswer(new Answer[java.util.concurrent.Future[RecordMetadata]]() {
          override def answer(invocation: InvocationOnMock) = {
            import java.util.concurrent.Executors
            Executors.newSingleThreadExecutor.submit(() => {
              Thread.sleep(10)
              val recordMetadata =
                new RecordMetadata(new TopicPartition("output_topic", 1), 1L, 1L, 1L, 1L.asInstanceOf[java.lang.Long], 1, 1)
              val exception = new RuntimeException("something bad happened!")
              invocation.getArgument[Callback](1).onCompletion(recordMetadata, exception)
              recordMetadata
            })
          }
        }).when(kafkaProducer).send(any[ProducerRecord[Int, String]], any[Callback])

        whenReady(kafkaProducerActor ? ProduceRecord(kRecords.head, outTopic) failed) { exception =>
          exception shouldBe a[KafkaProduceException]
        }
      }

      "fail with a KafkaProduceException if the producer fail to send a sequence of record in transaction" in {

        when(kafkaProducerOption.isTransactional).thenReturn(true)

        when(kafkaProducer.commitTransaction()).thenThrow(new RuntimeException("something bad heppened!"))

        whenReady(kafkaProducerActor ? ProduceRecords(kRecords, outTopic) failed) { exception =>
          exception shouldBe a[KafkaProduceException]
          verify(kafkaProducer).beginTransaction()
          kRecords.foreach { record =>
            verify(kafkaProducer).send(anyProducerRecordWith(record))
          }
          verify(kafkaProducer).commitTransaction()
          verify(kafkaProducer).abortTransaction()
        }
      }

    }

    "producing and committing records in transaction" should {

      "send the records to the producer and mark them as consumed" in {

        when(kafkaProducerOption.isTransactional).thenReturn(true)

        when(kafkaProducer.send(any[ProducerRecord[Int, String]])).thenReturn(aRecordMetadataFuture)

        whenReady(kafkaProducerActor ? ProduceRecordsAndCommit(kRecords, outTopic)) { _ =>
          verify(kafkaProducer).beginTransaction()
          kRecords.foreach { record =>
            verify(kafkaProducer).send(anyProducerRecordWith(record))
          }
          verify(kafkaProducer).sendOffsetsToTransaction(getOffsetsAndPartitions(kRecords),
                                                         kRecords.last.metadata.consumedByGroup)
          verify(kafkaProducer).commitTransaction()
        }

      }

      "fail with a KafkaProduceException if the producer fail and abort transaction" in {

        when(kafkaProducerOption.isTransactional).thenReturn(true)

        when(kafkaProducer.send(any[ProducerRecord[Int, String]])).thenReturn(aRecordMetadataFuture)
        when(kafkaProducer.commitTransaction()).thenThrow(new RuntimeException("Something bad happened!"))

        whenReady(kafkaProducerActor ? ProduceRecordsAndCommit(kRecords, outTopic) failed) { exception =>
          verify(kafkaProducer).beginTransaction()
          kRecords.foreach { record =>
            verify(kafkaProducer).send(anyProducerRecordWith(record))
          }
          verify(kafkaProducer).sendOffsetsToTransaction(getOffsetsAndPartitions(kRecords),
                                                         kRecords.last.metadata.consumedByGroup)
          verify(kafkaProducer).commitTransaction()
          verify(kafkaProducer).abortTransaction()

          exception shouldBe a[KafkaProduceException]
        }

      }

      "fail with a KafkaProduceException if the send fail and abort transaction" in {

        when(kafkaProducerOption.isTransactional).thenReturn(true)

        when(kafkaProducer.send(any[ProducerRecord[Int, String]])).thenThrow(new RuntimeException("Something bad happened!"))

        whenReady(kafkaProducerActor ? ProduceRecordsAndCommit(kRecords, outTopic) failed) { exception =>
          verify(kafkaProducer).beginTransaction()
          verify(kafkaProducer).send(any[ProducerRecord[Int, String]])
          verify(kafkaProducer, times(0)).sendOffsetsToTransaction(getOffsetsAndPartitions(kRecords),
                                                                   kRecords.last.metadata.consumedByGroup)
          verify(kafkaProducer, times(0)).commitTransaction()
          verify(kafkaProducer).abortTransaction()

          exception shouldBe a[KafkaProduceException]
        }
      }

      "fail with a KafkaProduceException if the commit fail and abort transaction" in {

        when(kafkaProducerOption.isTransactional).thenReturn(true)

        when(kafkaProducer.sendOffsetsToTransaction(getOffsetsAndPartitions(kRecords), kRecords.last.metadata.consumedByGroup))
          .thenThrow(new RuntimeException("Something bad happened!"))
        when(kafkaProducer.send(any[ProducerRecord[Int, String]])).thenReturn(aRecordMetadataFuture)

        whenReady(kafkaProducerActor ? ProduceRecordsAndCommit(kRecords, outTopic) failed) { exception =>
          verify(kafkaProducer).beginTransaction()
          kRecords.foreach { record =>
            verify(kafkaProducer).send(anyProducerRecordWith(record))
          }
          verify(kafkaProducer).sendOffsetsToTransaction(getOffsetsAndPartitions(kRecords),
                                                         kRecords.last.metadata.consumedByGroup)
          verify(kafkaProducer, times(0)).commitTransaction()
          verify(kafkaProducer).abortTransaction()

          exception shouldBe a[KafkaProduceException]
        }
      }

    }

    "receiving a unexpected message" should {

      "log a warning" in {
        EventFilter.warning(start = "Unexpected message:", occurrences = 1) intercept {
          kafkaProducerActor ! "UnexpectedMessage"
        }
      }

    }

  }

  private def anyProducerRecordWith[K, V](record: KRecord[K, V]) =
    argThat(new ArgumentMatcher[ProducerRecord[K, V]] {
      override def matches(argument: ProducerRecord[K, V]): Boolean =
        argument.value() == record.value && argument.key() == record.key

    })

}
