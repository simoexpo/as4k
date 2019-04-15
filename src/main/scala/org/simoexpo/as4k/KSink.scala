package org.simoexpo.as4k

import akka.Done
import akka.stream.scaladsl.{Flow, Keep, Sink}
import org.simoexpo.as4k.consumer.KafkaConsumerAgent
import org.simoexpo.as4k.model.KRecord
import org.simoexpo.as4k.producer.{KafkaSimpleProducerAgent, KafkaTransactionalProducerAgent}

import scala.concurrent.Future

object KSink {

  def produce[K, V](topic: String)(
      implicit kafkaProducerAgent: KafkaSimpleProducerAgent[K, V]): Sink[KRecord[K, V], Future[Done]] =
    Flow[KRecord[K, V]].mapAsync(1)(record => kafkaProducerAgent.produce(record, topic)).toMat(Sink.ignore)(Keep.right)

  def produceSequence[K, V](topic: String)(
      implicit kafkaProducerAgent: KafkaTransactionalProducerAgent[K, V]): Sink[Seq[KRecord[K, V]], Future[Done]] =
    Flow[Seq[KRecord[K, V]]].mapAsync(1)(record => kafkaProducerAgent.produce(record, topic)).toMat(Sink.ignore)(Keep.right)

  def produceAndCommit[K, V](topic: String)(
      implicit kafkaProducerAgent: KafkaTransactionalProducerAgent[K, V]): Sink[KRecord[K, V], Future[Done]] =
    Flow[KRecord[K, V]].mapAsync(1)(record => kafkaProducerAgent.produceAndCommit(record, topic)).toMat(Sink.ignore)(Keep.right)

  def produceSequenceAndCommit[K, V](topic: String)(
      implicit kafkaProducerAgent: KafkaTransactionalProducerAgent[K, V]): Sink[Seq[KRecord[K, V]], Future[Done]] =
    Flow[Seq[KRecord[K, V]]]
      .mapAsync(1)(record => kafkaProducerAgent.produceAndCommit(record, topic))
      .toMat(Sink.ignore)(Keep.right)

}
