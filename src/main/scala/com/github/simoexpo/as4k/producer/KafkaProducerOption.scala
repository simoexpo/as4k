package com.github.simoexpo.as4k.producer

import java.util.Properties

import org.apache.kafka.clients.producer.KafkaProducer

case class KafkaProducerOption[K, V](topic: String, isTransactional: Boolean) {

  @transient
  private lazy val props = {
//    val prop = new Properties()
//    this.getClass.getDeclaredFields.filter(field => KafkaConsumerOption.PropsField.contains(field.getName)).foreach { field =>
//      println(fieldToProp(field.getName))
//      field.get(this).asInstanceOf[Option[Any]].map { fieldValue =>
//        prop.put(fieldToProp(field.getName), fieldValue.toString)
//      }
//      prop.put("auto.offset.reset", "earliest")
//      //      prop.put("max.poll.records", "1000")
//    }
//    prop
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("acks", "all")
//    props.put("retries", 0.toString)
    props.put("batch.size", 16384.toString)
    props.put("linger.ms", 1.toString)
    props.put("buffer.memory", 33554432.toString)
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    if (isTransactional)
      props.put("transactional.id", "transaction_id")
    props
  }

  def createOne() = new KafkaProducer[K, V](props)

}

object KafkaProducerOption {

  def fromConfig(): KafkaProducerOption[Any, Any] = ???

}
