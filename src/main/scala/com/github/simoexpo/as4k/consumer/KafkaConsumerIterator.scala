package com.github.simoexpo.as4k.consumer

import com.github.simoexpo.as4k.consumer.KafkaConsumerActor.ConsumerToken

final private[as4k] class KafkaConsumerIterator extends Iterator[ConsumerToken] {

  override def hasNext: Boolean = true

  override def next(): ConsumerToken.type = ConsumerToken
}

private[as4k] object KafkaConsumerIterator {

  def getKafkaIterator: () => KafkaConsumerIterator = () => new KafkaConsumerIterator

}
