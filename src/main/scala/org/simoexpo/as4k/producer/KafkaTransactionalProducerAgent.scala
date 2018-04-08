package org.simoexpo.as4k.producer

import akka.actor.{ActorRef, ActorSystem, PoisonPill}
import akka.pattern.{ask, gracefulStop}
import akka.util.Timeout
import KafkaProducerActor.{ProduceRecords, ProduceRecordsAndCommit}
import org.simoexpo.as4k.model.KRecord

import scala.concurrent.{ExecutionContext, Future}

class KafkaTransactionalProducerAgent[K, V](producerOption: KafkaProducerOption[K, V])(implicit actorSystem: ActorSystem,
                                                                                       timeout: Timeout) {

  private implicit val ec: ExecutionContext = actorSystem.dispatcher

  def stopProducer: Future[Boolean] = gracefulStop(actor, timeout.duration, PoisonPill)

  protected val actor: ActorRef =
    actorSystem.actorOf(KafkaProducerActor.props(producerOption))

  def produce(records: Seq[KRecord[K, V]]): Future[Seq[KRecord[K, V]]] =
    (actor ? ProduceRecords(records)).map(_ => records)

  def produceAndCommit(record: KRecord[K, V], consumerGroup: String): Future[KRecord[K, V]] =
    (actor ? ProduceRecordsAndCommit(List(record), consumerGroup)).map(_ => record)

  def produceAndCommit(records: Seq[KRecord[K, V]], consumerGroup: String): Future[Seq[KRecord[K, V]]] =
    (actor ? ProduceRecordsAndCommit(records, consumerGroup)).map(_ => records)
}
