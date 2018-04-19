package org.simoexpo.as4k.producer

import akka.actor.{ActorRef, ActorSystem, PoisonPill}
import akka.pattern.{AskTimeoutException, ask, gracefulStop}
import akka.util.Timeout
import KafkaProducerActor.{ProduceRecords, ProduceRecordsAndCommit}
import org.simoexpo.as4k.model.KRecord
import org.simoexpo.as4k.producer.KafkaTransactionalProducerAgent.KafkaTransactionalProducerTimeoutException

import scala.concurrent.{ExecutionContext, Future}

class KafkaTransactionalProducerAgent[K, V](producerOption: KafkaProducerOption[K, V])(implicit actorSystem: ActorSystem,
                                                                                       timeout: Timeout) {

  private implicit val ec: ExecutionContext = actorSystem.dispatcher

  def stopProducer: Future[Boolean] = gracefulStop(actor, timeout.duration, PoisonPill)

  protected val actor: ActorRef =
    actorSystem.actorOf(KafkaProducerActor.props(producerOption))

  def produce(records: Seq[KRecord[K, V]]): Future[Seq[KRecord[K, V]]] =
    (actor ? ProduceRecords(records)).map(_ => records).recoverWith {
      case ex: AskTimeoutException => Future.failed(KafkaTransactionalProducerTimeoutException(timeout, ex))
    }

  def produceAndCommit(record: KRecord[K, V]): Future[KRecord[K, V]] =
    (actor ? ProduceRecordsAndCommit(List(record))).map(_ => record).recoverWith {
      case ex: AskTimeoutException => Future.failed(KafkaTransactionalProducerTimeoutException(timeout, ex))
    }

  def produceAndCommit(records: Seq[KRecord[K, V]]): Future[Seq[KRecord[K, V]]] =
    (actor ? ProduceRecordsAndCommit(records)).map(_ => records).recoverWith {
      case ex: AskTimeoutException => Future.failed(KafkaTransactionalProducerTimeoutException(timeout, ex))
    }
}

object KafkaTransactionalProducerAgent {

  case class KafkaTransactionalProducerTimeoutException(timeout: Timeout, ex: Throwable)
      extends RuntimeException(s"A timeout occurred when try to get response from kafka after ${timeout.duration} caused by: $ex")

}
