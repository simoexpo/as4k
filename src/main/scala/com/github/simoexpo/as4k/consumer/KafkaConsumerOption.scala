package com.github.simoexpo.as4k.consumer

import java.util.Properties

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.Deserializer

case class KafkaConsumerOption[K, V] private (clientId: Option[String],
                                              groupId: Option[String],
                                              topics: Option[List[String]],
                                              bootstrapServers: Option[String],
                                              enableAutoCommit: Option[Boolean],
                                              autoCommitIntervalMs: Option[Int],
                                              keyDeserializer: Option[Deserializer[K]],
                                              valueDeserializer: Option[Deserializer[V]]) {

  @transient
  private lazy val props = {
    val prop = new Properties()
    this.getClass.getDeclaredFields.filter(field => KafkaConsumerOption.PropsField.contains(field.getName)).foreach { field =>
      println(fieldToProp(field.getName))
      field.get(this).asInstanceOf[Option[Any]].map { fieldValue =>
        prop.put(fieldToProp(field.getName), fieldValue.toString)
      }
      prop.put("auto.offset.reset", "earliest")
//      prop.put("max.poll.records", "1000")
    }
    prop
  }

  def createOne(): KafkaConsumer[K, V] = new KafkaConsumer[K, V](props, keyDeserializer.orNull, valueDeserializer.orNull)

  private def fieldToProp(field: String) =
    "[A-Z\\d]".r.replaceAllIn(field, { m =>
      "." + m.group(0).toLowerCase()
    })

}

object KafkaConsumerOption {

  val PropsField = Seq("clientId", "groupId", "bootstrapServers", "enableAutoCommit", "autoCommitIntervalMs")

  def fromConfig(): KafkaConsumerOption[Any, Any] = ???

  def apply[K, V](clientId: String,
                  groupId: String,
                  topics: List[String],
                  bootstrapServers: String,
                  enableAutoCommit: Boolean,
                  autoCommitIntervalMs: Option[Int],
                  keyDeserializer: Deserializer[K],
                  valueDeserializer: Deserializer[V]): KafkaConsumerOption[K, V] =
    new KafkaConsumerOption[K, V](
      Some(clientId),
      Some(groupId),
      Some(topics),
      Some(bootstrapServers),
      Some(enableAutoCommit),
      autoCommitIntervalMs,
      Some(keyDeserializer),
      Some(valueDeserializer)
    )

}
