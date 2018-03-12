package com.github.simoexpo.as4k

import akka.stream.scaladsl.Source
import com.github.simoexpo.as4k.consumer.{KafkaConsumerAgent, KafkaConsumerIterator}
import com.github.simoexpo.as4k.factory.KRecord
import org.apache.kafka.clients.consumer._

import scala.concurrent.ExecutionContext
import scala.language.implicitConversions

object KSource {

  def fromKafkaConsumer[K, V](kafkaConsumerAgent: KafkaConsumerAgent[K, V])(
      implicit ec: ExecutionContext): Source[KRecord[K, V], Any] =
    Source
      .fromIterator(KafkaConsumerIterator.getKafkaIterator)
      .mapAsync(1) { token =>
        kafkaConsumerAgent.askForRecords(token)
      }
      .mapConcat(identity)

  implicit class KRecordSourceConverter[K, V](stream: Source[KRecord[K, V], Any]) {

    def commit(kafkaConsumerAgent: KafkaConsumerAgent[K, V]): Source[KRecord[K, V], Any] =
      stream.mapAsync(1)(kafkaConsumerAgent.commit)

    def commitAsync(kafkaConsumerAgent: KafkaConsumerAgent[K, V], callback: OffsetCommitCallback): Source[KRecord[K, V], Any] =
      stream.mapAsync(1)(record => kafkaConsumerAgent.commitAsync(record, callback))

    def mapValue[Out](fun: V => Out): Source[KRecord[K, Out], Any] =
      stream.map(_.mapValue(fun))

  }

  implicit class KRecordSeqSourceConverter[K, V](stream: Source[Seq[KRecord[K, V]], Any]) {

    def commit(kafkaConsumerAgent: KafkaConsumerAgent[K, V]): Source[Seq[KRecord[K, V]], Any] =
      stream.mapAsync(1)(kafkaConsumerAgent.commit)

    def commitAsync(kafkaConsumerAgent: KafkaConsumerAgent[K, V],
                    callback: OffsetCommitCallback): Source[Seq[KRecord[K, V]], Any] =
      stream.mapAsync(1)(records => kafkaConsumerAgent.commitAsync(records, callback))

  }

}
