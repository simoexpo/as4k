package com.github.simoexpo.as4k

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.github.simoexpo.as4k.conversion.CommitCallback.CommitCallback
import com.github.simoexpo.as4k.consumer.{KafkaConsumerAgent, KafkaConsumerIterator}
import org.apache.kafka.clients.consumer._

import scala.language.implicitConversions
import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext

object KSource {

  def fromKafkaConsumer[K, V](kafkaConsumerAgent: KafkaConsumerAgent[K, V])(
      implicit ec: ExecutionContext): Source[ConsumerRecord[K, V], NotUsed] =
    Source
      .fromIterator(KafkaConsumerIterator.getKafkaIterator)
      .mapAsync(1) { token =>
        kafkaConsumerAgent.askForRecords(token).map(_.asInstanceOf[ConsumerRecords[K, V]])
      }
      .mapConcat(_.iterator().asScala.toList)

  implicit class KSourceConsumerRecordConverter[K, V](s: Source[ConsumerRecord[K, V], NotUsed]) {

    def commit(kafkaConsumerAgent: KafkaConsumerAgent[K, V]): Source[ConsumerRecord[K, V], NotUsed] =
      s.mapAsync(1)(kafkaConsumerAgent.commit)

    def commitAsync(kafkaConsumerAgent: KafkaConsumerAgent[K, V],
                    callback: CommitCallback): Source[ConsumerRecord[K, V], NotUsed] =
      s.mapAsync(1)(record => kafkaConsumerAgent.commitAsync(record, callback))

  }

  implicit class KSourceConsumerRecordsConverter[K, V](s: Source[Seq[ConsumerRecord[K, V]], NotUsed]) {

    def commit(kafkaConsumerAgent: KafkaConsumerAgent[K, V]): Source[Seq[ConsumerRecord[K, V]], NotUsed] =
      s.mapAsync(1)(kafkaConsumerAgent.commit)

    def commitAsync(kafkaConsumerAgent: KafkaConsumerAgent[K, V],
                    callback: CommitCallback): Source[Seq[ConsumerRecord[K, V]], NotUsed] =
      s.mapAsync(1)(records => kafkaConsumerAgent.commitAsync(records, callback))

  }

}
