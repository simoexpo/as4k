package com.github.simoexpo.as4k.producer

import akka.actor.{Actor, ActorLogging, Props}
import com.github.simoexpo.as4k.factory.KRecord
import com.github.simoexpo.as4k.producer.KafkaProducerActor.{
  InitProducer,
  KafkaProduceException,
  ProduceRecords,
  ProduceRecordsAndCommit
}
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

private[as4k] class KafkaProducerActor[K, V](producerOption: KafkaProducerOption[K, V]) extends Actor with ActorLogging {

  implicit private val ec: ExecutionContext = context.dispatcher
  val producer: KafkaProducer[K, V] = producerOption.createOne()
  private val topic = producerOption.topic

  self ! InitProducer

  override def receive: Receive = {
    case InitProducer => producer.initTransactions()
    case ProduceRecords(records, callback) =>
      val originalSender = sender()
      Future.traverse(records.asInstanceOf[Seq[KRecord[K, V]]])(produce(callback)).map(_ => originalSender ! ()).recover {
        case NonFatal(ex) => originalSender ! akka.actor.Status.Failure(KafkaProduceException(ex))
      }
    case ProduceRecordsAndCommit(records, consumerGroup, callback) =>
      val originalSender = sender()
      produceAndCommit(records.asInstanceOf[Seq[KRecord[K, V]]], consumerGroup, callback).map(_ => originalSender ! ()).recover {
        case NonFatal(ex) => originalSender ! akka.actor.Status.Failure(KafkaProduceException(ex))
      }
  }

  private def produce(callback: Option[Callback])(record: KRecord[K, V]): Future[RecordMetadata] =
    Future(producer.send(new ProducerRecord(topic, record.key, record.value), callback.orNull).get()).recoverWith {
      case NonFatal(ex) =>
        log.error(s"Failed to produce $record on topic $topic: $ex")
        Future.failed(ex)
    }

  private def produceAndCommit(records: Seq[KRecord[K, V]], consumerGroup: String, callback: Option[Callback]): Future[Unit] =
    Future {
      producer.beginTransaction()
      records.foreach { record =>
        producer.sendOffsetsToTransaction(committableMetadata(record), consumerGroup)
        producer.send(new ProducerRecord(topic, record.key, record.value), callback.orNull)
      }
      producer.commitTransaction()

    }.recoverWith {
      case NonFatal(ex) =>
        log.error(s"Failed to produce $records on topic $topic: $ex")
        producer.abortTransaction()
        Future.failed(ex)
    }

  private def committableMetadata(record: KRecord[K, V]) = {
    val topicPartition = new TopicPartition(record.topic, record.partition)
    val offsetAndMetadata = new OffsetAndMetadata(record.offset + 1)
    Map(topicPartition -> offsetAndMetadata).asJava
  }

}

private[as4k] object KafkaProducerActor {

  case class ProduceRecordsAndCommit[K, V](records: Seq[KRecord[K, V]], consumerGroup: String, callback: Option[Callback] = None)
  case class ProduceRecords[K, V](records: Seq[KRecord[K, V]], callback: Option[Callback] = None)

  case class KafkaProduceException(exception: Throwable) extends RuntimeException(s"Failed to produce records: $exception")

  case object InitProducer

  def props[K, V](consumerOption: KafkaProducerOption[K, V]): Props =
    Props(new KafkaProducerActor(consumerOption))

}
