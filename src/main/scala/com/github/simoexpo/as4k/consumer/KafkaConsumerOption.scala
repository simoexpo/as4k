package com.github.simoexpo.as4k.consumer

import org.apache.kafka.clients.consumer.KafkaConsumer

case class KafkaConsumerOption[K, V]() {

  def createOne(): KafkaConsumer[K, V] = ???

}

object KafkaConsumerOption {

  def fromConfig(): KafkaConsumerOption[Any, Any] = ???

}
