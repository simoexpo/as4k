package com.github.simoexpo.as4k.producer

import akka.actor.Props
import akka.pattern.ask
import com.github.simoexpo.as4k.DataHelperSpec
import com.github.simoexpo.as4k.factory.KRecord
import com.github.simoexpo.as4k.producer.KafkaProducerActor.{KafkaProduceException, ProduceRecords, ProduceRecordsAndCommit}
import com.github.simoexpo.{ActorSystemSpec, BaseSpec}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.mockito.ArgumentMatcher
import org.mockito.Mockito._
import org.mockito.ArgumentMatchers.{any, argThat, isNull}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}

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

  when(kafkaProducerOption.topic).thenReturn("producerTopic")
  when(kafkaProducerOption.createOne()).thenReturn(kafkaProducer)

  private val kafkaProducerActor = system.actorOf(Props(new KafkaProducerActor(kafkaProducerOption)))

  override def beforeEach(): Unit =
    reset(kafkaProducer)

  "KafkaProducerActor" should {

    val topic = "topic"
    val partition = 1

    val kRecords = Range(0, 100).map(n => aKRecord(n, n, s"value$n", topic, partition)).toList

    "producing records" should {

      "send the records to the producer" in {

        when(kafkaProducer.send(any[ProducerRecord[Int, String]], isNull())).thenReturn(aRecordMetadataFuture)

        whenReady(kafkaProducerActor ? ProduceRecords(kRecords)) { _ =>
          kRecords.foreach { record =>
            verify(kafkaProducer).send(anyProducerRecordWith(record), isNull())
          }
        }
      }

      "fail with a KafkaProduceException if the producer fail" in {

        when(kafkaProducer.send(any[ProducerRecord[Int, String]], isNull())).thenReturn(aFailedRecordMetadataFuture)

        whenReady(kafkaProducerActor ? ProduceRecords(kRecords) failed) { exception =>
          exception shouldBe a[KafkaProduceException]
        }
      }

    }

    "producing and committing records in transaction" should {

      "send the records to the producer and mark them as consumed" in {

        val consumerGroup = "consumerGroup"

        when(kafkaProducer.send(any[ProducerRecord[Int, String]], isNull())).thenReturn(aRecordMetadataFuture)

        whenReady(kafkaProducerActor ? ProduceRecordsAndCommit(kRecords, consumerGroup)) { _ =>
          verify(kafkaProducer).beginTransaction()
          kRecords.foreach { record =>
            verify(kafkaProducer).sendOffsetsToTransaction(committableMetadata(record), consumerGroup)
            verify(kafkaProducer).send(anyProducerRecordWith(record), isNull())
          }
          verify(kafkaProducer).commitTransaction()
        }

      }

      "fail with a KafkaProduceException if the producer fail and abort transaction" in {

        val consumerGroup = "consumerGroup"

        when(kafkaProducer.send(any[ProducerRecord[Int, String]], isNull())).thenReturn(aRecordMetadataFuture)
        when(kafkaProducer.commitTransaction()).thenThrow(new RuntimeException("Something bad happened!"))

        whenReady(kafkaProducerActor ? ProduceRecordsAndCommit(kRecords, consumerGroup) failed) { exception =>
          verify(kafkaProducer).beginTransaction()
          kRecords.foreach { record =>
            verify(kafkaProducer).sendOffsetsToTransaction(committableMetadata(record), consumerGroup)
            verify(kafkaProducer).send(anyProducerRecordWith(record), isNull())
          }
          verify(kafkaProducer).commitTransaction()
          verify(kafkaProducer).abortTransaction()

          exception shouldBe a[KafkaProduceException]
        }

      }

      "fail with a KafkaProduceException if the commit fail and abort transaction" in {

        val consumerGroup = "consumerGroup"
        val failedRecordIndex = 20

        when(kafkaProducer.sendOffsetsToTransaction(committableMetadata(kRecords(failedRecordIndex)), consumerGroup))
          .thenThrow(new RuntimeException("Something bad happened!"))
        when(kafkaProducer.send(any[ProducerRecord[Int, String]], isNull())).thenReturn(aRecordMetadataFuture)

        whenReady(kafkaProducerActor ? ProduceRecordsAndCommit(kRecords, consumerGroup) failed) { exception =>
          verify(kafkaProducer).beginTransaction()
          kRecords.slice(0, failedRecordIndex).foreach { record =>
            verify(kafkaProducer).sendOffsetsToTransaction(committableMetadata(record), consumerGroup)
            verify(kafkaProducer).send(anyProducerRecordWith(record), isNull())
          }
          verify(kafkaProducer).sendOffsetsToTransaction(committableMetadata(kRecords(failedRecordIndex)), consumerGroup)
          verify(kafkaProducer, times(0)).commitTransaction()
          verify(kafkaProducer).abortTransaction()

          exception shouldBe a[KafkaProduceException]
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
