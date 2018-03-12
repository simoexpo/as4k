package com.github.simoexpo.as4k

import akka.Done
import akka.stream.scaladsl.Sink
import com.github.simoexpo.as4k.factory.KRecord
import com.github.simoexpo.as4k.producer.KafkaProducerAgent

import scala.concurrent.Future

object KSink {

  def produce[K, V](kafkaProducerAgent: KafkaProducerAgent[K, V]): Sink[KRecord[K, V], Future[Done]] =
    Sink.foreach[KRecord[K, V]] { t =>
      kafkaProducerAgent.produce(t)
    }

  def produceSequence[K, V](kafkaProducerAgent: KafkaProducerAgent[K, V]): Sink[Seq[KRecord[K, V]], Future[Done]] =
    Sink.foreach[Seq[KRecord[K, V]]] { t =>
      kafkaProducerAgent.produce(t)
    }

  def produceAndCommit[K, V](kafkaProducerAgent: KafkaProducerAgent[K, V]): Sink[KRecord[K, V], Future[Done]] =
    Sink.foreach[KRecord[K, V]] { t =>
      kafkaProducerAgent.produceAndCommit(t)
    }

  def produceSequenceAndCommit[K, V](kafkaProducerAgent: KafkaProducerAgent[K, V]): Sink[Seq[KRecord[K, V]], Future[Done]] =
    Sink.foreach[Seq[KRecord[K, V]]] { t =>
      kafkaProducerAgent.produceAndCommit(t)
    }

}
