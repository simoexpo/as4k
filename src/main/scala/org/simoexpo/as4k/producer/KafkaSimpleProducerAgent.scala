package org.simoexpo.as4k.producer

import akka.actor.{ActorRef, ActorSystem, PoisonPill}
import akka.pattern.{ask, gracefulStop}
import akka.util.Timeout
import KafkaProducerActor.ProduceRecord
import org.simoexpo.as4k.model.KRecord

import scala.concurrent.{ExecutionContext, Future}

class KafkaSimpleProducerAgent[K, V](producerOption: KafkaProducerOption[K, V])(implicit actorSystem: ActorSystem,
                                                                                timeout: Timeout) {

  private implicit val ec: ExecutionContext = actorSystem.dispatcher

  def stopProducer: Future[Boolean] = gracefulStop(actor, timeout.duration, PoisonPill)

  protected val actor: ActorRef =
    actorSystem.actorOf(KafkaProducerActor.props(producerOption))

  def produce(record: KRecord[K, V]): Future[KRecord[K, V]] =
    (actor ? ProduceRecord(record)).map(_ => record)

}
