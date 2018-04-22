package org.simoexpo.as4k.consumer

import java.util
import java.util.concurrent.TimeUnit

import akka.Done
import akka.actor.{Actor, ActorLogging, ActorRef, Props, Stash, Status}
import org.apache.kafka.clients.consumer.{KafkaConsumer, OffsetAndMetadata, OffsetCommitCallback}
import org.apache.kafka.common.TopicPartition
import org.simoexpo.as4k.consumer.KafkaConsumerActor._
import org.simoexpo.as4k.model.CustomCallback.CustomCommitCallback
import org.simoexpo.as4k.model.KRecord

import scala.collection.JavaConverters._
import scala.util.Try
import scala.util.control.NonFatal

private[as4k] class KafkaConsumerActor[K, V](consumerOption: KafkaConsumerOption[K, V])
    extends Actor
    with Stash
    with ActorLogging {

  protected val consumer: KafkaConsumer[K, V] = consumerOption.createOne()

  protected var pendingCommit = 0

  protected var assignment: util.Set[TopicPartition] = _

  self ! Subscribe(consumerOption.topics)

  override def receive: Receive = {
    case Subscribe(topics) =>
      log.info("Subscribed to topic {}", topics)
      consumer.subscribe(topics.asJava)
      assignment = consumer.assignment()
      unstashAll()
      context.become(initialized)
    case _: KafkaConsumerMessage => stash()
  }

  def initialized: Receive = {

    case ConsumerToken =>
      Try {
        consumer.resume(consumerAssignment)
        consumer
          .poll(consumerOption.pollingTimeout)
          .iterator()
          .asScala
          .map(consumedRecord => KRecord(consumedRecord, consumerOption.groupId))
          .toList
      }.map(sender() ! _).recover {
        case NonFatal(ex) => sender() ! Status.Failure(KafkaPollingException(ex))
      }

    case CommitOffsets(records, customCallback) =>
      records match {
        case Nil => sender() ! Done
        case _ :+ last =>
          commitRecord(self, sender(), last.asInstanceOf[KRecord[K, V]], customCallback).map { _ =>
            addPendingCommit(sender(), last.asInstanceOf[KRecord[K, V]])
          }.recover {
            case NonFatal(ex) => sender() ! Status.Failure(KafkaCommitException(ex))
          }
      }

    case PollToCommit if pendingCommit > 0 =>
      consumer.pause(consumerAssignment)
      consumer.poll(0)
      self ! PollToCommit

    case PollToCommit => ()

    case CommitComplete =>
      pendingCommit -= 1

    case msg => log.warning("Unexpected message: {}", msg)
  }

  private def consumerAssignment = {
    if (assignment.isEmpty)
      assignment = consumer.assignment()
    assignment
  }

  private def committableMetadata(record: KRecord[K, V]) = {
    val topicPartition = new TopicPartition(record.metadata.topic, record.metadata.partition)
    val offsetAndMetadata = new OffsetAndMetadata(record.metadata.offset + 1)
    Map(topicPartition -> offsetAndMetadata).asJava
  }

  private def commitCallback(consumerActor: ActorRef, originalSender: ActorRef, customCallback: Option[CustomCommitCallback]) =
    new OffsetCommitCallback {
      override def onComplete(offsets: util.Map[TopicPartition, OffsetAndMetadata], exception: Exception): Unit = {
        Option(exception) match {
          case None =>
            originalSender ! Done
            consumerActor ! CommitComplete
          case Some(ex) =>
            originalSender ! Status.Failure(KafkaCommitException(ex))
            consumerActor ! CommitComplete
        }
        customCallback.foreach(callback => callback(offsets.asScala.toMap, Option(exception)))
      }
    }

  private def commitRecord(consumerActor: ActorRef,
                           originalSender: ActorRef,
                           record: KRecord[K, V],
                           customCallback: Option[CustomCommitCallback]) =
    Try {
      val offset = committableMetadata(record)
      val callback = commitCallback(consumerActor, originalSender, customCallback)
      consumer.commitAsync(offset, callback)
    }

  private def addPendingCommit(sender: ActorRef, record: KRecord[K, V]): Unit = {
    if (pendingCommit == 0) {
      self ! PollToCommit
    }
    pendingCommit += 1
    ()
  }

  override def postStop(): Unit = {
    log.info(s"Terminating consumer...")
    consumer.close(1000, TimeUnit.MILLISECONDS)
    super.postStop()
  }

}

private[as4k] object KafkaConsumerActor {

  sealed trait KafkaConsumerMessage

  case object ConsumerToken extends KafkaConsumerMessage
  case class CommitOffsets[K, V](record: Seq[KRecord[K, V]], callback: Option[CustomCommitCallback] = None)
      extends KafkaConsumerMessage
  case class Subscribe(topics: Seq[String]) extends KafkaConsumerMessage
  case object PollToCommit extends KafkaConsumerMessage
  case object CommitComplete extends KafkaConsumerMessage

  case class KafkaPollingException(ex: Throwable) extends RuntimeException(s"Fail to poll new records: $ex")

  case class KafkaCommitException(ex: Throwable) extends RuntimeException(s"Fail to commit records: $ex")

  case class KafkaCommitTimeoutException[K, V](record: KRecord[K, V], timeout: Long)
      extends RuntimeException(s"Timeout Exception during the commit of $record: it took more than $timeout ms")

  def props[K, V](consumerOption: KafkaConsumerOption[K, V]): Props =
    Props(new KafkaConsumerActor(consumerOption))

}
