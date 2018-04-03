package com.github.simoexpo.as4k.producer

import java.util.Properties

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.serialization.Serializer

import scala.collection.JavaConverters._

case class KafkaProducerOption[K, V](topic: String,
                                     producerSetting: Map[String, String],
                                     dispatcher: Option[String],
                                     keySerializer: Option[Serializer[K]],
                                     valueSerializer: Option[Serializer[V]]) {

  @transient
  lazy val isTransactional: Boolean = producerSetting.get("transactional.id").isDefined

  def createOne() =
    new KafkaProducer[K, V](producerSetting.asInstanceOf[Map[String, Object]].asJava,
                            keySerializer.orNull,
                            valueSerializer.orNull)

}

object KafkaProducerOption {

  def apply[K, V](topic: String,
                  config: String,
                  keySerializer: Option[Serializer[K]] = None,
                  valueSerializer: Option[Serializer[V]] = None): KafkaProducerOption[K, V] =
    pureconfig.loadConfig[ProducerConf](config) match {
      case Right(conf) =>
        val kafkaProducerSetting = conf.producerSetting.map { element =>
          (element._1.replaceAll("-", "."), element._2)
        }
        new KafkaProducerOption(topic, kafkaProducerSetting, conf.dispatcher, keySerializer, valueSerializer)
      case Left(ex) => throw new IllegalArgumentException(s"Cannot load producer setting from $config: $ex")
    }

  private final case class ProducerConf(producerSetting: Map[String, String], dispatcher: Option[String])

}
