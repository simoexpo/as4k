package com.github.simoexpo.as4k

import akka.Done
import akka.stream.scaladsl.Sink
import com.github.simoexpo.as4k.consumer.KafkaConsumerAgent
import com.github.simoexpo.as4k.model.KRecord
import com.github.simoexpo.as4k.producer.{KafkaSimpleProducerAgent, KafkaTransactionalProducerAgent}

import scala.concurrent.Future

object KSink {

  def produce[K, V](kafkaProducerAgent: KafkaSimpleProducerAgent[K, V]): Sink[KRecord[K, V], Future[Done]] =
    Sink.foreach[KRecord[K, V]] { t =>
      kafkaProducerAgent.produce(t)
    }

  def produceSequence[K, V](kafkaProducerAgent: KafkaTransactionalProducerAgent[K, V]): Sink[Seq[KRecord[K, V]], Future[Done]] =
    Sink.foreach[Seq[KRecord[K, V]]] { t =>
      kafkaProducerAgent.produce(t)
    }

  def produceAndCommit[K, V](kafkaProducerAgent: KafkaTransactionalProducerAgent[K, V],
                             kafkaConsumerAgent: KafkaConsumerAgent[K, V]): Sink[KRecord[K, V], Future[Done]] =
    Sink.foreach[KRecord[K, V]] { t =>
      kafkaProducerAgent.produceAndCommit(t, kafkaConsumerAgent.consumerGroup)
    }

  def produceSequenceAndCommit[K, V](kafkaProducerAgent: KafkaTransactionalProducerAgent[K, V],
                                     kafkaConsumerAgent: KafkaConsumerAgent[K, V]): Sink[Seq[KRecord[K, V]], Future[Done]] =
    Sink.foreach[Seq[KRecord[K, V]]] { t =>
      kafkaProducerAgent.produceAndCommit(t, kafkaConsumerAgent.consumerGroup)
    }

}
