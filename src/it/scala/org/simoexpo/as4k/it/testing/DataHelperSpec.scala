package org.simoexpo.as4k.it.testing

import org.simoexpo.as4k.consumer.KafkaConsumerOption
import org.simoexpo.as4k.producer.KafkaProducerOption

import scala.util.Random

trait DataHelperSpec {

  def getKafkaSimpleConsumerOption(topics: String*): KafkaConsumerOption[String, String] = {
    val kafkaConsumerOption: KafkaConsumerOption[String, String] = KafkaConsumerOption(topics, "my-consumer")
    val consumerGroup = s"consumer_group_${Random.nextInt().abs}"
    kafkaConsumerOption.copy(consumerSetting = kafkaConsumerOption.consumerSetting + ("group.id" -> consumerGroup))
  }

  def getKafkaTransactionalConsumerOption(topics: String*): KafkaConsumerOption[String, String] = {
    val kafkaConsumerOption: KafkaConsumerOption[String, String] = KafkaConsumerOption(topics, "my-transaction-consumer")
    val consumerGroup = s"consumer_group_${Random.nextInt().abs}"
    kafkaConsumerOption.copy(consumerSetting = kafkaConsumerOption.consumerSetting + ("group.id" -> consumerGroup))
  }

  def getKafkaSimpleProducerOption: KafkaProducerOption[String, String] =
    KafkaProducerOption("my-simple-producer")

  def getKafkaTransactionalProducerOption: KafkaProducerOption[String, String] =
    KafkaProducerOption("my-transactional-producer")

  def getRandomTopicName = s"topic_${Random.nextInt().abs}"

}
