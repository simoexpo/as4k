package com.github.simoexpo.as4k.producer

import akka.actor.{ActorRef, ActorSystem}
import akka.pattern.ask
import akka.util.Timeout
import com.github.simoexpo.as4k.model.KRecord
import com.github.simoexpo.as4k.producer.KafkaProducerActor.{ProduceRecords, ProduceRecordsAndCommit}

import scala.concurrent.{ExecutionContext, Future}

class KafkaTransactionalProducerAgent[K, V](producerOption: KafkaProducerOption[K, V])(implicit actorSystem: ActorSystem,
                                                                                       timeout: Timeout) {

  private implicit val ec: ExecutionContext = actorSystem.dispatcher

  protected val actor: ActorRef =
    actorSystem.actorOf(KafkaProducerActor.props(producerOption))

  def produce(records: Seq[KRecord[K, V]]): Future[Seq[KRecord[K, V]]] =
    (actor ? ProduceRecords(records)).map(_ => records)

  def produceAndCommit(record: KRecord[K, V], consumerGroup: String): Future[KRecord[K, V]] =
    (actor ? ProduceRecordsAndCommit(List(record), consumerGroup)).map(_ => record)

  def produceAndCommit(records: Seq[KRecord[K, V]], consumerGroup: String): Future[Seq[KRecord[K, V]]] =
    (actor ? ProduceRecordsAndCommit(records, consumerGroup)).map(_ => records)
}
