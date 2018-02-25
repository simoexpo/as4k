package com.github.simoexpo.as4k

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.github.simoexpo.as4k.consumer.{KafkaConsumerAgent, KafkaConsumerIterator}
import com.github.simoexpo.as4k.factory.KRecord
import org.apache.kafka.clients.consumer._

import scala.language.implicitConversions
import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext

object KSource {

  def fromKafkaConsumer[K, V](kafkaConsumerAgent: KafkaConsumerAgent[K, V])(
      implicit ec: ExecutionContext): Source[KRecord[K, V], NotUsed] =
    Source
      .fromIterator(KafkaConsumerIterator.getKafkaIterator)
      .mapAsync(1) { token =>
        kafkaConsumerAgent.askForRecords(token).map(_.asInstanceOf[ConsumerRecords[K, V]])
      }
      .mapConcat(_.iterator().asScala.map(KRecord(_)).toList)

  implicit class KSourceConsumerRecordConverter[K, V](s: Source[KRecord[K, V], NotUsed]) {

    def commit(kafkaConsumerAgent: KafkaConsumerAgent[K, V]): Source[KRecord[K, V], NotUsed] =
      s.mapAsync(1)(kafkaConsumerAgent.commit)

    def commitAsync(kafkaConsumerAgent: KafkaConsumerAgent[K, V],
                    callback: OffsetCommitCallback): Source[KRecord[K, V], NotUsed] =
      s.mapAsync(1)(record => kafkaConsumerAgent.commitAsync(record, callback))

    def mapValue[Out](fun: V => Out): Source[KRecord[K, Out], NotUsed] =
      s.map(_.mapValue(fun))

  }

  implicit class KSourceConsumerRecordsConverter[K, V](s: Source[Seq[KRecord[K, V]], NotUsed]) {

    def commit(kafkaConsumerAgent: KafkaConsumerAgent[K, V]): Source[Seq[KRecord[K, V]], NotUsed] =
      s.mapAsync(1)(kafkaConsumerAgent.commit)

    def commitAsync(kafkaConsumerAgent: KafkaConsumerAgent[K, V],
                    callback: OffsetCommitCallback): Source[Seq[KRecord[K, V]], NotUsed] =
      s.mapAsync(1)(records => kafkaConsumerAgent.commitAsync(records, callback))

  }

}
