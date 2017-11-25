package com.github.simoexpo.as4k.conversion

import java.util

import org.apache.kafka.clients.consumer.{OffsetAndMetadata, OffsetCommitCallback}
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConverters._
import scala.language.implicitConversions

object CommitCallback {

  type CommitCallback = (Map[TopicPartition, OffsetAndMetadata], Option[Exception]) => Unit

  private[as4k] implicit def toOffsetCommitCallback(
      callback: (Map[TopicPartition, OffsetAndMetadata], Option[Exception]) => Unit): OffsetCommitCallback =
    new OffsetCommitCallback {
      override def onComplete(offsets: util.Map[TopicPartition, OffsetAndMetadata], exception: Exception): Unit =
        callback(offsets.asScala.toMap, Option(exception))
    }

}
