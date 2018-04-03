package com.github.simoexpo.as4k.producer

import com.github.simoexpo.BaseSpec

class KafkaProducerOptionSpec extends BaseSpec {

  "KafkaProducerOption" should {

    val topic = "topic"

    "create a KafkaProducer with the correct setting" in {

      val kafkaProducerOption = KafkaProducerOption(topic, "my-simple-producer")

      val props = kafkaProducerOption.producerSetting

      noException should be thrownBy kafkaProducerOption.createOne()

      kafkaProducerOption.isTransactional shouldBe false
      kafkaProducerOption.topic shouldBe topic
      props("bootstrap.servers") shouldBe "127.0.0.1:9092"
      props("acks") shouldBe "all"
      props("batch.size") shouldBe "16384"
      props("linger.ms") shouldBe "1"
      props("buffer.memory") shouldBe "33554432"
      props("key.serializer") shouldBe "org.apache.kafka.common.serialization.StringSerializer"
      props("value.serializer") shouldBe "org.apache.kafka.common.serialization.StringSerializer"
    }

    "create a KafkaProducer with the correct setting if transactional" in {

      val kafkaProducerOption = KafkaProducerOption(topic, "my-transactional-producer")

      val props = kafkaProducerOption.producerSetting

      noException should be thrownBy kafkaProducerOption.createOne()

      kafkaProducerOption.isTransactional shouldBe true
      kafkaProducerOption.topic shouldBe topic
      props("bootstrap.servers") shouldBe "127.0.0.1:9092"
      props("acks") shouldBe "all"
      props("batch.size") shouldBe "16384"
      props("linger.ms") shouldBe "1"
      props("buffer.memory") shouldBe "33554432"
      props("key.serializer") shouldBe "org.apache.kafka.common.serialization.StringSerializer"
      props("value.serializer") shouldBe "org.apache.kafka.common.serialization.StringSerializer"
      props("transactional.id") shouldBe "transaction_id"
    }

    "fail to create KafkaConsumerOption if no consumer setting are found" in {

      an[Exception] should be thrownBy KafkaProducerOption(topic, "no-setting")

    }

  }
}
