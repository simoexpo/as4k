package org.simoexpo.as4k.model

import org.apache.kafka.clients.consumer.ConsumerRecord

final case class KRecord[K, V](key: K, value: V, metadata: KRecordMetadata) {

  def mapValue[Out](f: V => Out): KRecord[K, Out] =
    this.copy(value = f(value))

}

object KRecord {

  def apply[K, V](record: ConsumerRecord[K, V], consumedByGroup: String): KRecord[K, V] = {
    val metadata = KRecordMetadata(record.topic(), record.partition(), record.offset(), record.timestamp(), consumedByGroup)
    KRecord(record.key(), record.value(), metadata)
  }

}

final case class KRecordMetadata(topic: String, partition: Int, offset: Long, timestamp: Long, consumedByGroup: String)
