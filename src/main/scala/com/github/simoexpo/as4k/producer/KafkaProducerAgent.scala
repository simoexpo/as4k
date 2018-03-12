package com.github.simoexpo.as4k.producer

import akka.actor.{ActorRef, ActorSystem}
import akka.util.Timeout
import akka.pattern.ask
import com.github.simoexpo.as4k.factory.KRecord
import com.github.simoexpo.as4k.producer.KafkaProducerActor.{ProduceRecords, ProduceRecordsInTransaction}

import scala.concurrent.{ExecutionContext, Future}

class KafkaProducerAgent[K, V](producerOption: KafkaProducerOption[K, V])(implicit actorSystem: ActorSystem, timeout: Timeout) {

  private implicit val ec: ExecutionContext = actorSystem.dispatcher

  protected val actor: ActorRef = actorSystem.actorOf(KafkaProducerActor.props(producerOption))

  def produce(record: KRecord[K, V]): Future[KRecord[K, V]] =
    (actor ? ProduceRecords(List(record))).map(_ => record)

  def produce(records: Seq[KRecord[K, V]]): Future[Seq[KRecord[K, V]]] =
    (actor ? ProduceRecords(records)).map(_ => records)

  def produceAndCommit(record: KRecord[K, V]): Future[KRecord[K, V]] =
    (actor ? ProduceRecordsInTransaction(List(record))).map(_ => record)

  def produceAndCommit(records: Seq[KRecord[K, V]]): Future[Seq[KRecord[K, V]]] =
    (actor ? ProduceRecordsInTransaction(records)).map(_ => records)

}
