package org.simoexpo.as4k.consumer

import akka.actor.{ActorRef, ActorSystem, PoisonPill}
import akka.pattern.{AskTimeoutException, ask, gracefulStop}
import akka.util.Timeout
import org.simoexpo.as4k.consumer.KafkaConsumerActor.{CommitOffsets, ConsumerToken}
import org.simoexpo.as4k.consumer.KafkaConsumerAgent.KafkaConsumerTimeoutException
import org.simoexpo.as4k.model.CustomCallback.CustomCommitCallback
import org.simoexpo.as4k.model.KRecord

import scala.concurrent.{ExecutionContext, Future}
import scala.language.implicitConversions

class KafkaConsumerAgent[K, V](consumerOption: KafkaConsumerOption[K, V])(implicit actorSystem: ActorSystem, timeout: Timeout) {
  private implicit val ec: ExecutionContext = actorSystem.dispatcher

  val consumerGroup: String = consumerOption.groupId
  private val dispatcherConfig = consumerOption.dispatcher

  protected val actor: ActorRef = {
    val props = dispatcherConfig match {
      case Some(dispatcher) =>
        KafkaConsumerActor.props(consumerOption).withDispatcher(dispatcher)
      case None =>
        KafkaConsumerActor.props(consumerOption)
    }
    actorSystem.actorOf(props)
  }

  def stopConsumer: Future[Boolean] = gracefulStop(actor, timeout.duration, PoisonPill)

  def askForRecords: Future[List[KRecord[K, V]]] =
    (actor ? ConsumerToken).map(_.asInstanceOf[List[KRecord[K, V]]]).recoverWith {
      case ex: AskTimeoutException => Future.failed(KafkaConsumerTimeoutException(timeout, ex))
    }

  def commit(record: KRecord[K, V], callback: Option[CustomCommitCallback] = None): Future[KRecord[K, V]] =
    (actor ? CommitOffsets(Seq(record), callback)).map(_ => record).recoverWith {
      case ex: AskTimeoutException => Future.failed(KafkaConsumerTimeoutException(timeout, ex))
    }

  def commitBatch(records: Seq[KRecord[K, V]], callback: Option[CustomCommitCallback] = None): Future[Seq[KRecord[K, V]]] =
    (actor ? CommitOffsets(records, callback)).map(_ => records).recoverWith {
      case ex: AskTimeoutException => Future.failed(KafkaConsumerTimeoutException(timeout, ex))
    }

}

object KafkaConsumerAgent {

  case class KafkaConsumerTimeoutException(timeout: Timeout, ex: Throwable)
      extends RuntimeException(s"A timeout occurred when try to get response from kafka after ${timeout.duration} caused by: $ex")

}
