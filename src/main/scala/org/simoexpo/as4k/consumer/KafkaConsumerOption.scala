package org.simoexpo.as4k.consumer

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.Deserializer

import scala.collection.JavaConverters._

case class KafkaConsumerOption[K, V] private (topics: Seq[String],
                                              consumerSetting: Map[String, String],
                                              dispatcher: Option[String],
                                              keyDeserializer: Option[Deserializer[K]],
                                              valueDeserializer: Option[Deserializer[V]]) {

  @transient
  lazy val groupId: Option[String] = consumerSetting.get("group.id")

  def createOne(): KafkaConsumer[K, V] =
    new KafkaConsumer[K, V](consumerSetting.asInstanceOf[Map[String, Object]].asJava,
                            keyDeserializer.orNull,
                            valueDeserializer.orNull)

}

object KafkaConsumerOption {

  def apply[K, V](topic: Seq[String],
                  config: String,
                  keyDeserializer: Option[Deserializer[K]] = None,
                  valueDeserializer: Option[Deserializer[V]] = None): KafkaConsumerOption[K, V] =
    pureconfig.loadConfig[ConsumerConf](config) match {
      case Right(conf) =>
        val kafkaConsumerSetting = conf.consumerSetting.map { element =>
          (element._1.replaceAll("-", "."), element._2)
        }
        new KafkaConsumerOption(topic, kafkaConsumerSetting, conf.dispatcher, keyDeserializer, valueDeserializer)
      case Left(ex) => throw new IllegalArgumentException(s"Cannot load consumer setting from $config: $ex")
    }

  private final case class ConsumerConf(consumerSetting: Map[String, String], dispatcher: Option[String])

}
