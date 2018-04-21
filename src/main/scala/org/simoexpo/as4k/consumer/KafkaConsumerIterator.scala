package org.simoexpo.as4k.consumer

final private[as4k] class KafkaConsumerIterator extends Iterator[Unit] {

  override def hasNext: Boolean = true

  override def next(): Unit = ()
}

private[as4k] object KafkaConsumerIterator {

  def createOne: () => KafkaConsumerIterator = () => new KafkaConsumerIterator

}
