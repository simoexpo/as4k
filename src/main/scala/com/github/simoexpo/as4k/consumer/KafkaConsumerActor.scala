package com.github.simoexpo.as4k.consumer

import akka.actor.{Actor, ActorLogging, Props}
import com.github.simoexpo.as4k.consumer.KafkaConsumerActor._
import com.github.simoexpo.as4k.factory.KRecord
import org.apache.kafka.clients.consumer.{KafkaConsumer, OffsetAndMetadata, OffsetCommitCallback}
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConverters._
import scala.util.Try
import scala.util.control.NonFatal

private[as4k] class KafkaConsumerActor[K, V](consumerOption: KafkaConsumerOption[K, V], pollingInterval: Long)
    extends Actor
    with ActorLogging {

  protected val consumer: KafkaConsumer[K, V] = consumerOption.createOne()

  override def receive: Receive = {
    case ConsumerToken =>
      Try(consumer.poll(pollingInterval).iterator().asScala.map(KRecord(_)).toList).map(sender() ! _).recover {
        case NonFatal(ex) => sender() ! akka.actor.Status.Failure(KafkaPollingException(ex))
      }
    case CommitOffsetSync(records) =>
      Try(records.asInstanceOf[Seq[KRecord[K, V]]].map(committableMetadata).foreach(consumer.commitSync))
        .map(_ => sender() ! ())
        .recover {
          case NonFatal(ex) => sender() ! akka.actor.Status.Failure(KafkaCommitException(ex))
        }
    case CommitOffsetAsync(records, callback) =>
      Try(
        records
          .asInstanceOf[Seq[KRecord[K, V]]]
          .map(committableMetadata)
          .foreach(offset => consumer.commitAsync(offset, callback))).map(_ => sender() ! ()).recover {
        case NonFatal(ex) => sender() ! akka.actor.Status.Failure(KafkaCommitException(ex))
      }
  }

  private def committableMetadata(record: KRecord[K, V]) = {
    val topicPartition = new TopicPartition(record.topic, record.partition)
    val offsetAndMetadata = new OffsetAndMetadata(record.offset + 1)
    Map(topicPartition -> offsetAndMetadata).asJava
  }

}

private[as4k] object KafkaConsumerActor {

  case object ConsumerToken
  type ConsumerToken = ConsumerToken.type

  case class CommitOffsetSync[K, V](records: Seq[KRecord[K, V]])

  case class CommitOffsetAsync[K, V](records: Seq[KRecord[K, V]], callback: OffsetCommitCallback)

  case class KafkaPollingException(ex: Throwable) extends RuntimeException(s"Fail to poll new records: $ex")

  case class KafkaCommitException(ex: Throwable) extends RuntimeException(s"Fail to poll new records: $ex")

  def props[K, V](consumerOption: KafkaConsumerOption[K, V], pollingInterval: Long): Props =
    Props(new KafkaConsumerActor(consumerOption, pollingInterval))

}
