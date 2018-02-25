package com.github.simoexpo.as4k.consumer

import akka.actor.{Actor, ActorLogging, Props}
import com.github.simoexpo.as4k.consumer.KafkaConsumerActor.{CommitOffsetAsync, CommitOffsetSync, ConsumerToken}
import com.github.simoexpo.as4k.factory.KRecord
import org.apache.kafka.clients.consumer.{KafkaConsumer, OffsetAndMetadata, OffsetCommitCallback}
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConverters._

final private[as4k] class KafkaConsumerActor[K, V](private val consumer: KafkaConsumer[K, V], private val pollingInterval: Long)
    extends Actor
    with ActorLogging {

  override def receive: Receive = {
    case ConsumerToken => sender() ! consumer.poll(pollingInterval)
    case CommitOffsetSync(records) =>
      records.asInstanceOf[Seq[KRecord[K, V]]].map(committableMetadata).foreach(consumer.commitSync)
      sender() ! ()
    case CommitOffsetAsync(records, callback) =>
      records.asInstanceOf[Seq[KRecord[K, V]]].map(committableMetadata).foreach(offset => consumer.commitAsync(offset, callback))
      sender() ! ()
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

  def props[K, V](kafkaConsumer: KafkaConsumer[K, V], pollingInterval: Long): Props =
    Props(new KafkaConsumerActor(kafkaConsumer, pollingInterval))

}
