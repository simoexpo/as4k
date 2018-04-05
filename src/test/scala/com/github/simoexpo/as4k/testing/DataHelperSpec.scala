package com.github.simoexpo.as4k.testing

import java.util.concurrent.{Future => JavaFuture, TimeUnit => JavaTimeUnit}

import com.github.simoexpo.as4k.model.KRecord
import org.apache.kafka.clients.consumer.{ConsumerRecord, OffsetAndMetadata}
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConverters._

trait DataHelperSpec {

  protected def aConsumerRecord[K, V](offset: Long, key: K, value: V, topic: String, partition: Int) =
    new ConsumerRecord(topic, partition, offset, key, value)

  protected def aKRecord[K, V](offset: Long, key: K, value: V, topic: String, partition: Int) =
    KRecord(key, value, topic, partition, offset, System.currentTimeMillis())

  protected def committableMetadata[K, V](record: KRecord[K, V]) = {
    val topicPartition = new TopicPartition(record.topic, record.partition)
    val offsetAndMetadata = new OffsetAndMetadata(record.offset + 1)
    Map(topicPartition -> offsetAndMetadata).asJava
  }

  protected def aRecordMetadataFuture: JavaFuture[RecordMetadata] =
    new JavaFuture[RecordMetadata] {
      override def isCancelled: Boolean = throw new UnsupportedOperationException

      override def get(): RecordMetadata = new RecordMetadata(new TopicPartition("topic", 1), 1, 1, 1, 1, 1, 1)

      override def get(timeout: Long, unit: JavaTimeUnit): RecordMetadata =
        new RecordMetadata(new TopicPartition("topic", 1), 1, 1, 1, 1, 1, 1)

      override def cancel(mayInterruptIfRunning: Boolean): Boolean = throw new UnsupportedOperationException

      override def isDone: Boolean = true
    }

  protected def aFailedRecordMetadataFuture: JavaFuture[RecordMetadata] =
    new JavaFuture[RecordMetadata] {
      override def isCancelled: Boolean = throw new UnsupportedOperationException

      override def get(): RecordMetadata = throw new RuntimeException()

      override def get(timeout: Long, unit: JavaTimeUnit): RecordMetadata = throw new RuntimeException()

      override def cancel(mayInterruptIfRunning: Boolean): Boolean = throw new UnsupportedOperationException

      override def isDone: Boolean = true
    }
}
