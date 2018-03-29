package com.github.simoexpo.as4k.factory

import java.util

import org.apache.kafka.clients.consumer.{OffsetAndMetadata, OffsetCommitCallback}
import org.apache.kafka.clients.producer.{Callback, RecordMetadata}
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConverters._
import scala.language.implicitConversions

object CallbackFactory {

  type CustomCommitCallback = (Map[TopicPartition, OffsetAndMetadata], Option[Exception]) => Unit

  def apply(callback: CustomCommitCallback): OffsetCommitCallback =
    new OffsetCommitCallback {
      override def onComplete(offsets: util.Map[TopicPartition, OffsetAndMetadata], exception: Exception): Unit =
        callback(offsets.asScala.toMap, Option(exception))
    }

  def apply(callback: (RecordMetadata, Option[Exception]) => Unit): Callback =
    new Callback {
      override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit =
        callback(metadata, Option(exception))
    }

}
