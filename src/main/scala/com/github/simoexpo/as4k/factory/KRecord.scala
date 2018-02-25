package com.github.simoexpo.as4k.factory

import org.apache.kafka.clients.consumer.ConsumerRecord

case class KRecord[K, V](key: K, value: V, topic: String, partition: Int, offset: Long, timestamp: Long) {

  def mapValue[Out](f: V => Out): KRecord[K, Out] =
    this.copy(value = f(value))

}

object KRecord {

  def apply[K, V](record: ConsumerRecord[K, V]): KRecord[K, V] =
    KRecord(record.key(), record.value(), record.topic(), record.partition(), record.offset(), record.timestamp())

}
