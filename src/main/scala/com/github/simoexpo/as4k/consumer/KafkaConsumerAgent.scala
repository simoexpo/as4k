package com.github.simoexpo.as4k.consumer

import akka.actor.{ActorRef, ActorSystem}
import akka.pattern.ask
import akka.util.Timeout
import com.github.simoexpo.as4k.conversion.CommitCallback.CommitCallback
import com.github.simoexpo.as4k.conversion.CommitCallback._
import com.github.simoexpo.as4k.consumer.KafkaConsumerActor.{CommitOffsetAsync, CommitOffsetSync, ConsumerToken}
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}

import scala.concurrent.{ExecutionContext, Future}
import scala.language.implicitConversions

class KafkaConsumerAgent[K, V](kafkaConsumer: KafkaConsumer[K, V], pollingInterval: Long)(implicit actorSystem: ActorSystem,
                                                                                          timeout: Timeout) {

  private implicit val ec: ExecutionContext = actorSystem.dispatcher

  protected val actor: ActorRef = actorSystem.actorOf(KafkaConsumerActor.props(kafkaConsumer, pollingInterval))

  def askForRecords(token: ConsumerToken): Future[Any] = actor ? token

  def commit(record: ConsumerRecord[K, V]): Future[ConsumerRecord[K, V]] =
    (actor ? CommitOffsetSync(List(record))).map(_ => record)

  def commit(records: Seq[ConsumerRecord[K, V]]): Future[Seq[ConsumerRecord[K, V]]] =
    (actor ? CommitOffsetSync(records)).map(_ => records)

  def commitAsync(record: ConsumerRecord[K, V], callback: CommitCallback): Future[ConsumerRecord[K, V]] =
    (actor ? CommitOffsetAsync(List(record), callback)).map(_ => record)

  def commitAsync(records: Seq[ConsumerRecord[K, V]], callback: CommitCallback): Future[Seq[ConsumerRecord[K, V]]] =
    (actor ? CommitOffsetAsync(records, callback)).map(_ => records)

}
