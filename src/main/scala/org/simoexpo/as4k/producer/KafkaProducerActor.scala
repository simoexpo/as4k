package org.simoexpo.as4k.producer

import java.time.Duration
import java.util

import akka.Done
import akka.actor.{Actor, ActorLogging, ActorRef, Props, Stash, Status}
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.TopicPartition
import org.simoexpo.as4k.model.CustomCallback.CustomSendCallback
import org.simoexpo.as4k.model.KRecord
import org.simoexpo.as4k.producer.KafkaProducerActor._

import scala.collection.JavaConverters._
import scala.util.Try
import scala.util.control.NonFatal

private[as4k] class KafkaProducerActor[K, V](producerOption: KafkaProducerOption[K, V])
    extends Actor
    with ActorLogging
    with Stash {

  protected val producer: KafkaProducer[K, V] = producerOption.createOne()
  protected val topic: String = producerOption.topic

  self ! InitProducer

  override def receive: Receive = {
    case InitProducer =>
      log.info("Initializing producer...")
      if (producerOption.isTransactional)
        producer.initTransactions()
      unstashAll()
      context.become(initialized)
    case _ => stash()
  }

  def initialized: Receive = {
    case ProduceRecord(record, callback) if !producerOption.isTransactional =>
      produce(record.asInstanceOf[KRecord[K, V]], callback)

    case ProduceRecords(records) if producerOption.isTransactional =>
      produceInTransaction(records.asInstanceOf[Seq[KRecord[K, V]]])

    case ProduceRecordsAndCommit(records) if producerOption.isTransactional =>
      produceInTransaction(records.asInstanceOf[Seq[KRecord[K, V]]], true)

    case msg => log.warning("Unexpected message: {}", msg)
  }

  private def produce(record: KRecord[K, V], customCallback: Option[CustomSendCallback]): Try[Unit] =
    Try {
      val callback = sendCallback(self, sender(), customCallback)
      producer.send(new ProducerRecord(topic, record.key, record.value), callback)
      ()
    }.recover {
      case NonFatal(ex) => sender() ! Status.Failure(KafkaProduceException(ex))
    }

  private def sendCallback(consumerActor: ActorRef,
                           originalSender: ActorRef,
                           customCallback: Option[CustomSendCallback]): Callback =
    new Callback {
      override def onCompletion(recordMetadata: RecordMetadata, exception: Exception): Unit = {
        Option(exception) match {
          case None =>
            originalSender ! Done
          case Some(ex) =>
            originalSender ! Status.Failure(KafkaProduceException(ex))
        }
        customCallback.foreach(callback => callback(recordMetadata, Option(exception)))
      }
    }

  private def produceInTransaction(records: Seq[KRecord[K, V]], commit: Boolean = false): Try[Unit] =
    Try {
      producer.beginTransaction()
      records.foreach(record => producer.send(new ProducerRecord(topic, record.key, record.value)))
      if (commit && records.nonEmpty)
        producer.sendOffsetsToTransaction(getOffsetsAndPartitions(records), records.head.metadata.consumedBy)
      producer.commitTransaction()
      sender() ! Done
    }.recover {
      case NonFatal(ex) =>
        log.error(s"Failed to produce $records on topic $topic: $ex")
        producer.abortTransaction()
        sender() ! Status.Failure(KafkaProduceException(ex))
    }

  private def getOffsetsAndPartitions(records: Seq[KRecord[K, V]]): util.Map[TopicPartition, OffsetAndMetadata] = {
    val groupedRecords = records.groupBy(_.metadata.partition)
    groupedRecords.map {
      case (_, Nil) => None
      case (partition, _ :+ lastRecord) =>
        val topicPartition = new TopicPartition(lastRecord.metadata.topic, partition)
        val offsetAndMetadata = new OffsetAndMetadata(lastRecord.metadata.offset + 1)
        Some(topicPartition -> offsetAndMetadata)
    }.flatten.toMap.asJava
  }

  override def postStop(): Unit = {
    log.info(s"Terminating producer...")
    producer.close(Duration.ofMillis(1000))
    super.postStop()
  }
}

private[as4k] object KafkaProducerActor {

  case class ProduceRecordsAndCommit[K, V](records: Seq[KRecord[K, V]])
  case class ProduceRecords[K, V](records: Seq[KRecord[K, V]])
  case class ProduceRecord[K, V](record: KRecord[K, V], callback: Option[CustomSendCallback] = None)

  case class KafkaProduceException(exception: Throwable) extends RuntimeException(s"Failed to produce records: $exception")

  case object InitProducer

  def props[K, V](producerOption: KafkaProducerOption[K, V]): Props =
    Props(new KafkaProducerActor(producerOption))

}
