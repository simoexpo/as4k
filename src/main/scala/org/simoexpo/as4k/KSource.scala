package org.simoexpo.as4k

import akka.stream.scaladsl.Source
import org.simoexpo.as4k.consumer.{KafkaConsumerAgent, KafkaConsumerIterator}
import org.simoexpo.as4k.model.CustomCallback.CustomCommitCallback
import org.simoexpo.as4k.model.KRecord
import org.simoexpo.as4k.producer.{KafkaSimpleProducerAgent, KafkaTransactionalProducerAgent}

import scala.concurrent.ExecutionContext
import scala.language.implicitConversions

object KSource {

  def fromKafkaConsumer[K, V](kafkaConsumerAgent: KafkaConsumerAgent[K, V])(
      implicit ec: ExecutionContext): Source[KRecord[K, V], Any] =
    Source.fromIterator(KafkaConsumerIterator.createOne).mapAsync(1)(_ => kafkaConsumerAgent.askForRecords).mapConcat(identity)

  implicit class KRecordSourceConverter[K, V](stream: Source[KRecord[K, V], Any]) {

    def commit(parallelism: Int = 1)(kafkaConsumerAgent: KafkaConsumerAgent[K, V],
                                     customCallback: Option[CustomCommitCallback] = None): Source[KRecord[K, V], Any] =
      stream.mapAsync(parallelism)(record => kafkaConsumerAgent.commit(record, customCallback))

    def mapValue[Out](fun: V => Out): Source[KRecord[K, Out], Any] =
      stream.map(_.mapValue(fun))

    def produce(parallelism: Int = 1)(kafkaProducerAgent: KafkaSimpleProducerAgent[K, V]): Source[KRecord[K, V], Any] =
      stream.mapAsync(parallelism)(kafkaProducerAgent.produce)

    def produceAndCommit(kafkaProducerAgent: KafkaTransactionalProducerAgent[K, V]): Source[KRecord[K, V], Any] =
      stream.mapAsync(1)(record => kafkaProducerAgent.produceAndCommit(record))

  }

  implicit class KRecordSeqSourceConverter[K, V](stream: Source[Seq[KRecord[K, V]], Any]) {

    def commit(parallelism: Int = 1)(kafkaConsumerAgent: KafkaConsumerAgent[K, V],
                                     customCallback: Option[CustomCommitCallback] = None): Source[Seq[KRecord[K, V]], Any] =
      stream.mapAsync(parallelism) { records =>
        kafkaConsumerAgent.commitBatch(records, customCallback)
      }

    def produce(kafkaProducerAgent: KafkaTransactionalProducerAgent[K, V]): Source[Seq[KRecord[K, V]], Any] =
      stream.mapAsync(1)(kafkaProducerAgent.produce)

    def produceAndCommit(kafkaProducerAgent: KafkaTransactionalProducerAgent[K, V]): Source[Seq[KRecord[K, V]], Any] =
      stream.mapAsync(1)(records => kafkaProducerAgent.produceAndCommit(records))

  }

}
