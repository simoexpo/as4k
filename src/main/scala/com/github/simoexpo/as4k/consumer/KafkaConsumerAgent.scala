package com.github.simoexpo.as4k.consumer

import akka.actor.{ActorRef, ActorSystem}
import akka.pattern.ask
import akka.util.Timeout
import com.github.simoexpo.as4k.consumer.KafkaConsumerActor.{CommitOffsets, ConsumerToken}
import com.github.simoexpo.as4k.factory.CallbackFactory.CustomCommitCallback
import com.github.simoexpo.as4k.factory.KRecord

import scala.concurrent.{ExecutionContext, Future}
import scala.language.implicitConversions

class KafkaConsumerAgent[K, V](consumerOption: KafkaConsumerOption[K, V], pollingTimeout: Long)(implicit actorSystem: ActorSystem,
                                                                                                timeout: Timeout) {

  private implicit val ec: ExecutionContext = actorSystem.dispatcher

  val consumerGroup: String = consumerOption.groupId.getOrElse("defaultGroup")

  protected val actor: ActorRef =
    actorSystem.actorOf(KafkaConsumerActor.props(consumerOption, pollingTimeout).withDispatcher("consumer-dispatcher-2"))

  def askForRecords(token: ConsumerToken): Future[List[KRecord[K, V]]] =
    (actor ? token).map(_.asInstanceOf[List[KRecord[K, V]]])

  def commit(record: KRecord[K, V], callback: Option[CustomCommitCallback] = None): Future[KRecord[K, V]] =
    (actor ? CommitOffsets(Seq(record), callback)).map(_ => record)

  def commitBatch(records: Seq[KRecord[K, V]], callback: Option[CustomCommitCallback] = None): Future[Seq[KRecord[K, V]]] =
    (actor ? CommitOffsets(records, callback)).map(_ => records)

}
