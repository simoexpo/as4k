package com.github.simoexpo.as4k.producer

import org.apache.kafka.clients.producer.KafkaProducer

class KafkaProducerOption[K, V](val topic: String) {

  def createOne(): KafkaProducer[K, V] = ???

}

object KafkaProducerOption {

  def fromConfig(): KafkaProducerOption[Any, Any] = ???

}
