package com.github.simoexpo.as4k

import akka.stream.scaladsl.{RestartSource, Source}
import com.github.simoexpo.as4k.consumer.{KafkaConsumerAgent, KafkaConsumerIterator}
import com.github.simoexpo.as4k.factory.KRecord
import org.apache.kafka.clients.consumer._

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.language.implicitConversions

object KSource {

  lazy val MinBackoff: FiniteDuration = 10.seconds
  lazy val MaxBackoff: FiniteDuration = 60.seconds
  lazy val RandomFactor: Double = 0.2
  lazy val MaxRestart: Int = 2

  def fromKafkaConsumer[K, V](kafkaConsumerAgent: KafkaConsumerAgent[K, V])(
      implicit ec: ExecutionContext): Source[KRecord[K, V], Any] =
    RestartSource.withBackoff(
      minBackoff = MinBackoff,
      maxBackoff = MaxBackoff,
      randomFactor = RandomFactor,
      maxRestarts = MaxRestart
    ) { () =>
      Source
        .fromIterator(KafkaConsumerIterator.getKafkaIterator)
        .mapAsync(1) { token =>
          kafkaConsumerAgent.askForRecords(token)
        }
        .mapConcat(identity)
    }

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
