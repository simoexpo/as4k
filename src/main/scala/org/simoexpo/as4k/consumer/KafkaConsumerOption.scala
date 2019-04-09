package org.simoexpo.as4k.consumer

import java.time.Duration

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.Deserializer

import scala.collection.JavaConverters._
import scala.concurrent.duration.FiniteDuration

case class KafkaConsumerOption[K, V] private (topics: Seq[String],
                                              pollingTimeout: FiniteDuration,
                                              consumerSetting: Map[String, String],
                                              dispatcher: Option[String],
                                              keyDeserializer: Option[Deserializer[K]],
                                              valueDeserializer: Option[Deserializer[V]]) {

  @transient
  lazy val groupId: String = consumerSetting.getOrElse("group.id", "as4kDefaultConsumerGroup")

  def createOne(): KafkaConsumer[K, V] =
    new KafkaConsumer[K, V](consumerSetting.asInstanceOf[Map[String, Object]].asJava,
                            keyDeserializer.orNull,
                            valueDeserializer.orNull)

}

object KafkaConsumerOption {

  import pureconfig.generic.auto._
  import scala.concurrent.duration._
  import scala.language.postfixOps

  def apply[K, V](topic: Seq[String],
                  config: String,
                  pollingTimeout: FiniteDuration = 100 millis,
                  keyDeserializer: Option[Deserializer[K]] = None,
                  valueDeserializer: Option[Deserializer[V]] = None): KafkaConsumerOption[K, V] =
    pureconfig.loadConfig[ConsumerConf](config) match {
      case Right(conf) =>
        val kafkaConsumerSetting = conf.consumerSetting.map {
          case (key, value) => (key.replaceAll("-", "."), value)
        }
        new KafkaConsumerOption(topic, pollingTimeout, kafkaConsumerSetting, conf.dispatcher, keyDeserializer, valueDeserializer)
      case Left(ex) => throw new IllegalArgumentException(s"Cannot load consumer setting from $config: $ex")
    }

  private final case class ConsumerConf(consumerSetting: Map[String, String], dispatcher: Option[String])

}
