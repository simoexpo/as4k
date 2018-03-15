package com.github.simoexpo.as4k.consumer

import akka.actor.{ActorRef, ActorSystem}
import akka.pattern.ask
import akka.util.Timeout
import com.github.simoexpo.as4k.consumer.KafkaConsumerActor.{CommitOffsetAsync, CommitOffsetSync, ConsumerToken}
import com.github.simoexpo.as4k.factory.KRecord
import org.apache.kafka.clients.consumer.OffsetCommitCallback

import scala.concurrent.{ExecutionContext, Future}
import scala.language.implicitConversions

class KafkaConsumerAgent[K, V](consumerOption: KafkaConsumerOption[K, V], pollingTimeout: Long)(implicit actorSystem: ActorSystem,
                                                                                                timeout: Timeout) {

  private implicit val ec: ExecutionContext = actorSystem.dispatcher

  val consumerGroup: String = ""

  protected val actor: ActorRef = actorSystem.actorOf(KafkaConsumerActor.props(consumerOption, pollingTimeout))

  def askForRecords(token: ConsumerToken): Future[List[KRecord[K, V]]] =
    (actor ? token).map(_.asInstanceOf[List[KRecord[K, V]]])

  def commit(record: KRecord[K, V]): Future[KRecord[K, V]] =
    (actor ? CommitOffsetSync(List(record))).map(_ => record)

  def commit(records: Seq[KRecord[K, V]]): Future[Seq[KRecord[K, V]]] =
    (actor ? CommitOffsetSync(records)).map(_ => records)

  def commitAsync(record: KRecord[K, V], callback: OffsetCommitCallback): Future[KRecord[K, V]] =
    (actor ? CommitOffsetAsync(List(record), callback)).map(_ => record)

  def commitAsync(records: Seq[KRecord[K, V]], callback: OffsetCommitCallback): Future[Seq[KRecord[K, V]]] =
    (actor ? CommitOffsetAsync(records, callback)).map(_ => records)

}
