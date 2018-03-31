package com.github.simoexpo.as4k.producer

import akka.actor.{ActorRef, ActorSystem}
import akka.pattern.ask
import akka.util.Timeout
import com.github.simoexpo.as4k.model.KRecord
import com.github.simoexpo.as4k.producer.KafkaProducerActor.ProduceRecord

import scala.concurrent.{ExecutionContext, Future}

class KafkaSimpleProducerAgent[K, V](producerOption: KafkaProducerOption[K, V])(implicit actorSystem: ActorSystem,
                                                                                timeout: Timeout) {

  private implicit val ec: ExecutionContext = actorSystem.dispatcher

  protected val actor: ActorRef =
    actorSystem.actorOf(KafkaProducerActor.props(producerOption))

  def produce(record: KRecord[K, V]): Future[KRecord[K, V]] =
    (actor ? ProduceRecord(record)).map(_ => record)

}
