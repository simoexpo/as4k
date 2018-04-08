package org.simoexpo.as4k.model

import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.TopicPartition

import scala.language.implicitConversions

object CustomCallback {

  type CustomCommitCallback = (Map[TopicPartition, OffsetAndMetadata], Option[Exception]) => Unit
  type CustomSendCallback = (RecordMetadata, Option[Exception]) => Unit

}
