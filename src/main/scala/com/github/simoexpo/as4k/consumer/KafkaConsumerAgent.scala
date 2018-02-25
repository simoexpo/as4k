package com.github.simoexpo.as4k.consumer

import akka.actor.{ActorRef, ActorSystem}
import akka.pattern.ask
import akka.util.Timeout
import com.github.simoexpo.as4k.consumer.KafkaConsumerActor.{CommitOffsetAsync, CommitOffsetSync, ConsumerToken}
import com.github.simoexpo.as4k.factory.KRecord
import org.apache.kafka.clients.consumer.{KafkaConsumer, OffsetCommitCallback}

import scala.concurrent.{ExecutionContext, Future}
import scala.language.implicitConversions

class KafkaConsumerAgent[K, V](kafkaConsumer: KafkaConsumer[K, V], pollingInterval: Long)(implicit actorSystem: ActorSystem,
                                                                                          timeout: Timeout) {

  private implicit val ec: ExecutionContext = actorSystem.dispatcher

  protected val actor: ActorRef = actorSystem.actorOf(KafkaConsumerActor.props(kafkaConsumer, pollingInterval))

  def askForRecords(token: ConsumerToken): Future[Any] = actor ? token

  def commit(record: KRecord[K, V]): Future[KRecord[K, V]] =
    (actor ? CommitOffsetSync(List(record))).map(_ => record)

  def commit(records: Seq[KRecord[K, V]]): Future[Seq[KRecord[K, V]]] =
    (actor ? CommitOffsetSync(records)).map(_ => records)

  def commitAsync(record: KRecord[K, V], callback: OffsetCommitCallback): Future[KRecord[K, V]] =
    (actor ? CommitOffsetAsync(List(record), callback)).map(_ => record)

  def commitAsync(records: Seq[KRecord[K, V]], callback: OffsetCommitCallback): Future[Seq[KRecord[K, V]]] =
    (actor ? CommitOffsetAsync(records, callback)).map(_ => records)

}
