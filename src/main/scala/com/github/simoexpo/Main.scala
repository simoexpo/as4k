package com.github.simoexpo

import akka.NotUsed
import akka.actor.{Actor, ActorLogging}
import akka.stream.scaladsl.{Sink, Source}
import com.github.simoexpo.as4k.KSource
import com.github.simoexpo.as4k.KSource._
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer}

object Main extends App {

//  val kafkaConsumerAgent: KafkaConsumerAgent[String, String] = ???

//  val s = KSource.fromKafkaConsumer(kafkaConsumerAgent).log(_).runWith(Sink.ignore)

//  val z: ConsumerRecord[String, String] = ???

//  val c: String = z.key

}
