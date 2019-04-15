package org.simoexpo.as4k

import akka.stream.scaladsl.Sink
import org.mockito.Mockito.{atLeast => invokedAtLeast, _}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.concurrent.{IntegrationPatience, ScalaFutures}
import org.simoexpo.as4k.consumer.KafkaConsumerAgent
import org.simoexpo.as4k.model.KRecord
import org.simoexpo.as4k.testing.{ActorSystemSpec, BaseSpec, DataHelperSpec}

import scala.concurrent.Future

class KSourceSpec
    extends BaseSpec
    with ScalaFutures
    with ActorSystemSpec
    with IntegrationPatience
    with BeforeAndAfterEach
    with DataHelperSpec {

  private val kafkaConsumerAgent: KafkaConsumerAgent[Int, String] = mock[KafkaConsumerAgent[Int, String]]

  override def beforeEach(): Unit = reset(kafkaConsumerAgent)

  "KSource" when {

    val topic = "topic"
    val partitions = 3

    val records1 = Range(0, 100).map(n => aKRecord(n, n, s"value$n", topic, n % partitions, "defaultGroup")).toList
    val records2 = Range(100, 200).map(n => aKRecord(n, n, s"value$n", topic, n % partitions, "defaultGroup")).toList
    val records3 = Range(200, 220).map(n => aKRecord(n, n, s"value$n", topic, n % partitions, "defaultGroup")).toList

    "producing a source of KRecord" should {

      "ask kafka consumer for new records" in {

        val totalRecordsSize = Seq(records1, records2, records3).map(_.size).sum
        val expectedRecords = Seq(records1, records2, records3).flatten

        when(kafkaConsumerAgent.askForRecords)
          .thenReturn(Future.successful(records1))
          .thenReturn(Future.successful(records2))
          .thenReturn(Future.successful(records3))

        val recordsConsumed =
          KSource.fromKafkaConsumer(kafkaConsumerAgent).take(totalRecordsSize).runWith(Sink.seq)

        whenReady(recordsConsumed) { records =>
          records.size shouldBe totalRecordsSize
          records.toList shouldBe expectedRecords

          verify(kafkaConsumerAgent, invokedAtLeast(3)).askForRecords

        }
      }

      "not end when not receiving new records from kafka consumer agent" in {

        val totalRecordsSize = Seq(records1, records2).map(_.size).sum
        val expectedRecords = Seq(records1, records2).flatten

        val emptyRecords = List.empty[KRecord[Int, String]]

        when(kafkaConsumerAgent.askForRecords)
          .thenReturn(Future.successful(records1))
          .thenReturn(Future.successful(emptyRecords))
          .thenReturn(Future.successful(records2))

        val recordsConsumed = KSource.fromKafkaConsumer(kafkaConsumerAgent).take(totalRecordsSize).runWith(Sink.seq)

        whenReady(recordsConsumed) { records =>
          records.size shouldBe totalRecordsSize
          records.toList shouldBe expectedRecords

          verify(kafkaConsumerAgent, invokedAtLeast(3)).askForRecords

        }
      }
    }
  }
}
