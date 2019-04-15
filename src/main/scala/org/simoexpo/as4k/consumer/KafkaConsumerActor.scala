package org.simoexpo.as4k.consumer

import java.time.Duration
import java.util

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
          .poll(Duration.ofMillis(consumerOption.pollingTimeout.toMillis))
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
        case recordsList =>
          commitRecord(self, sender(), recordsList.asInstanceOf[Seq[KRecord[K, V]]], customCallback).map { _ =>
            addPendingCommit()
          }.recover {
            case NonFatal(ex) => sender() ! Status.Failure(KafkaCommitException(ex))
          }
      }

    case PollToCommit if pendingCommit > 0 =>
      consumer.pause(consumerAssignment)
      consumer.poll(Duration.ZERO)
      self ! PollToCommit

    case PollToCommit => ()

    case CommitComplete =>
      removePendingCommit()

    case msg => log.warning("Unexpected message: {}", msg)
  }

  private def consumerAssignment = {
    if (assignment.isEmpty)
      assignment = consumer.assignment()
    assignment
  }

  private def getOffsetsAndPartitions(records: Seq[KRecord[K, V]]): util.Map[TopicPartition, OffsetAndMetadata] = {
    val groupedRecords = records.groupBy(_.metadata.partition)
    groupedRecords.map {
      case (_, Nil) => None
      case (partition, _ :+ lastRecord) =>
        val topicPartition = new TopicPartition(lastRecord.metadata.topic, partition)
        val offsetAndMetadata = new OffsetAndMetadata(lastRecord.metadata.offset + 1)
        Some(topicPartition -> offsetAndMetadata)
    }.flatten.toMap.asJava
  }

  private def commitCallback(consumerActor: ActorRef,
                             originalSender: ActorRef,
                             customCallback: Option[CustomCommitCallback]): OffsetCommitCallback =
    (offsets: util.Map[TopicPartition, OffsetAndMetadata], exception: Exception) => {
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

  private def commitRecord(consumerActor: ActorRef,
                           originalSender: ActorRef,
                           records: Seq[KRecord[K, V]],
                           customCallback: Option[CustomCommitCallback]) =
    Try {
      val offsets = getOffsetsAndPartitions(records)
      val callback = commitCallback(consumerActor, originalSender, customCallback)
      consumer.commitAsync(offsets, callback)
    }

  private def addPendingCommit(): Unit = {
    if (pendingCommit == 0) {
      self ! PollToCommit
    }
    pendingCommit += 1
    ()
  }

  private def removePendingCommit(): Unit = {
    pendingCommit -= 1
    ()
  }

  override def postStop(): Unit = {
    log.info(s"Terminating consumer...")
    consumer.close(Duration.ofMillis(1000))
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
