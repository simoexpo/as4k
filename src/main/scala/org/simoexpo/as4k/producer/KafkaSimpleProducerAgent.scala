package org.simoexpo.as4k.producer

import akka.actor.{ActorRef, ActorSystem, PoisonPill}
import akka.pattern.{ask, gracefulStop, AskTimeoutException}
import akka.util.Timeout
import KafkaProducerActor.ProduceRecord
import org.simoexpo.as4k.model.KRecord
import org.simoexpo.as4k.producer.KafkaSimpleProducerAgent.KafkaSimpleProducerTimeoutException

import scala.concurrent.{ExecutionContext, Future}

class KafkaSimpleProducerAgent[K, V](producerOption: KafkaProducerOption[K, V])(implicit actorSystem: ActorSystem,
                                                                                timeout: Timeout) {

  private implicit val ec: ExecutionContext = actorSystem.dispatcher

  def stopProducer: Future[Boolean] = gracefulStop(actor, timeout.duration, PoisonPill)

  protected val actor: ActorRef =
    actorSystem.actorOf(KafkaProducerActor.props(producerOption))

  def produce(record: KRecord[K, V], topic: String): Future[KRecord[K, V]] =
    (actor ? ProduceRecord(record, topic)).map(_ => record).recoverWith {
      case ex: AskTimeoutException => Future.failed(KafkaSimpleProducerTimeoutException(timeout, ex))
    }

}

object KafkaSimpleProducerAgent {

  case class KafkaSimpleProducerTimeoutException(timeout: Timeout, ex: Throwable)
      extends RuntimeException(s"A timeout occurred when try to get response from kafka after ${timeout.duration} caused by: $ex")

}
