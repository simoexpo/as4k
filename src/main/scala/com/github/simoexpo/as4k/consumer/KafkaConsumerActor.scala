package com.github.simoexpo.as4k.consumer

import java.util
import java.util.concurrent.TimeUnit

import akka.Done
import akka.actor.{Actor, ActorLogging, ActorRef, Props, Stash, Status}
import com.github.simoexpo.as4k.consumer.KafkaConsumerActor._
import com.github.simoexpo.as4k.model.CustomCallback.CustomCommitCallback
import com.github.simoexpo.as4k.model.KRecord
import org.apache.kafka.clients.consumer.{KafkaConsumer, OffsetAndMetadata, OffsetCommitCallback}
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConverters._
import scala.util.Try
import scala.util.control.NonFatal

// TODO: test thread consumption, test thread in callback, timeout on waiting CommitComplete ?

private[as4k] class KafkaConsumerActor[K, V](consumerOption: KafkaConsumerOption[K, V], pollingTimeout: Long)
    extends Actor
    with Stash
    with ActorLogging {

  private val CommitTimeout: Long = 1000

  protected val consumer: KafkaConsumer[K, V] = consumerOption.createOne()

  protected var pendingCommit = 0

  protected var assignment: util.Set[TopicPartition] = _

//  var consumerTime = 0L
//
//  var consumerCount = 0L
//
//  var commitTime = 0L
//
//  var commitCount = 0L
//
//  var pollTime = 0L
//
//  var pollCount = 0L
//
//  var signalCommitEndTime = 0L
//
//  var signalCount = 0L

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
//      val s = System.currentTimeMillis()
      Try {
        consumer.resume(consumerAssignment)
        consumer.poll(pollingTimeout).iterator().asScala.map(KRecord(_)).toList
      }.map(sender() ! _).recover {
        case NonFatal(ex) => sender() ! Status.Failure(KafkaPollingException(ex))
      }
//      val e = System.currentTimeMillis()
//      consumerTime += e - s
//      consumerCount += 1

    case CommitOffsets(records, customCallback) =>
//      val s = System.currentTimeMillis()
      records match {
        case Nil => sender() ! Done
        case recordsList =>
          commitRecord(self, sender(), recordsList.last.asInstanceOf[KRecord[K, V]], customCallback).map { _ =>
            addPendingCommit(sender(), recordsList.last.asInstanceOf[KRecord[K, V]])
          }.recover {
            case NonFatal(ex) => sender() ! Status.Failure(KafkaCommitException(ex))
          }
      }
//      val e = System.currentTimeMillis()
//      commitCount += 1
//      commitTime += e - s

    case PollToCommit if pendingCommit > 0 =>
//      val s = System.currentTimeMillis()
      consumer.pause(consumerAssignment)
      consumer.poll(0)
      self ! PollToCommit
//      val e = System.currentTimeMillis()
//      pollCount += 1
//      pollTime += e - s

    case PollToCommit => ()

    case CommitComplete(reply, maybeException) =>
//      val s = System.currentTimeMillis()
      pendingCommit -= 1
//      val e = System.currentTimeMillis()
//      signalCount += 1
//      signalCommitEndTime += e - s

    case msg => log.warning("Unexpected message: {}", msg)
  }

  private def consumerAssignment = {
    if (assignment.isEmpty)
      assignment = consumer.assignment()
    assignment
  }

  private def committableMetadata(record: KRecord[K, V]) = {
    val topicPartition = new TopicPartition(record.topic, record.partition)
    val offsetAndMetadata = new OffsetAndMetadata(record.offset + 1)
    Map(topicPartition -> offsetAndMetadata).asJava
  }

  private def commitCallback(consumerActor: ActorRef, originalSender: ActorRef, customCallback: Option[CustomCommitCallback]) =
    new OffsetCommitCallback {
      override def onComplete(offsets: util.Map[TopicPartition, OffsetAndMetadata], exception: Exception): Unit = {
        Option(exception) match {
          case None =>
            originalSender ! Done
            consumerActor ! CommitComplete(originalSender)
          case Some(ex) =>
            originalSender ! Status.Failure(KafkaCommitException(ex))
            consumerActor ! CommitComplete(originalSender, Some(ex))
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
//    log.info(s"Consume: $consumerTime ms distributed between $consumerCount call")
//    log.info(s"Commit: $commitTime ms distributed between $commitCount call")
//    log.info(s"Poll: $pollTime ms distributed between $pollCount call")
//    log.info(s"SignalEnd: $signalCommitEndTime ms distributed between $signalCount call")
    consumer.close(1000, TimeUnit.MILLISECONDS)
    super.postStop()
  }

}

private[as4k] object KafkaConsumerActor {

  sealed trait KafkaConsumerMessage

  case object ConsumerToken extends KafkaConsumerMessage
  type ConsumerToken = ConsumerToken.type

  case class CommitOffsets[K, V](record: Seq[KRecord[K, V]], callback: Option[CustomCommitCallback] = None)
      extends KafkaConsumerMessage
  case class Subscribe(topics: Seq[String]) extends KafkaConsumerMessage
  case class StartCommit[K, V](originalSender: ActorRef, record: KRecord[K, V], callback: Option[CustomCommitCallback] = None)
      extends KafkaConsumerMessage
  case object PollToCommit extends KafkaConsumerMessage
  case class CommitComplete(reply: ActorRef, exception: Option[Exception] = None) extends KafkaConsumerMessage

  case class KafkaPollingException(ex: Throwable) extends RuntimeException(s"Fail to poll new records: $ex")

  case class KafkaCommitException(ex: Throwable) extends RuntimeException(s"Fail to commit records: $ex")

  case class KafkaCommitTimeoutException[K, V](record: KRecord[K, V], timeout: Long)
      extends RuntimeException(s"Timeout Exception during the commit of $record: it took more than $timeout ms")

  case class CommitOperation[K, V](sender: ActorRef, record: KRecord[K, V], timestamp: Long)

  def props[K, V](consumerOption: KafkaConsumerOption[K, V], pollingTimeout: Long): Props =
    Props(new KafkaConsumerActor(consumerOption, pollingTimeout))

}
