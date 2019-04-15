package org.simoexpo.as4k.consumer

import akka.actor.{ActorRef, ActorSystem, PoisonPill}
import akka.pattern.{ask, gracefulStop, AskTimeoutException}
import akka.util.Timeout
import org.simoexpo.as4k.consumer.KafkaConsumerActor.{CommitOffsets, ConsumerToken}
import org.simoexpo.as4k.consumer.KafkaConsumerAgent.KafkaConsumerTimeoutException
import org.simoexpo.as4k.model.CustomCallback.CustomCommitCallback
import org.simoexpo.as4k.model.{KRecord, NonEmptyList}

import scala.concurrent.{ExecutionContext, Future}
import scala.language.implicitConversions

class KafkaConsumerAgent[K, V](consumerOption: KafkaConsumerOption[K, V], consumerCount: Int = 1)(
    implicit actorSystem: ActorSystem,
    timeout: Timeout) {

  require(consumerCount > 0, "KafkaConsumerAgent should have at least one consumer")

  private implicit val ec: ExecutionContext = actorSystem.dispatcher

  val consumerGroup: String = consumerOption.groupId
  private val dispatcherConfig = consumerOption.dispatcher

  protected val actors: NonEmptyList[ActorRef] = {
    val props = dispatcherConfig match {
      case Some(dispatcher) =>
        KafkaConsumerActor.props(consumerOption).withDispatcher(dispatcher)
      case None =>
        KafkaConsumerActor.props(consumerOption)
    }
    NonEmptyList(actorSystem.actorOf(props), List.fill(consumerCount - 1)(actorSystem.actorOf(props)))
  }

  def stopConsumer: Future[Boolean] =
    Future.traverse(actors.values)(actor => gracefulStop(actor, timeout.duration, PoisonPill)).map(_.forall(identity))

  def askForRecords: Future[List[KRecord[K, V]]] =
    Future
      .traverse(actors.values) { actor =>
        (actor ? ConsumerToken).map(_.asInstanceOf[List[KRecord[K, V]]]).recoverWith {
          case ex: AskTimeoutException => Future.failed(KafkaConsumerTimeoutException(timeout, ex))
        }
      }
      .map(_.flatten)

  def commit(record: KRecord[K, V], callback: Option[CustomCommitCallback] = None): Future[KRecord[K, V]] =
    (actors.head ? CommitOffsets(Seq(record), callback)).map(_ => record).recoverWith {
      case ex: AskTimeoutException => Future.failed(KafkaConsumerTimeoutException(timeout, ex))
    }

  def commitBatch(records: Seq[KRecord[K, V]], callback: Option[CustomCommitCallback] = None): Future[Seq[KRecord[K, V]]] =
    (actors.head ? CommitOffsets(records, callback)).map(_ => records).recoverWith {
      case ex: AskTimeoutException => Future.failed(KafkaConsumerTimeoutException(timeout, ex))
    }

}

object KafkaConsumerAgent {

  case class KafkaConsumerTimeoutException(timeout: Timeout, ex: Throwable)
      extends RuntimeException(s"A timeout occurred when try to get response from kafka after ${timeout.duration} caused by: $ex")

}
