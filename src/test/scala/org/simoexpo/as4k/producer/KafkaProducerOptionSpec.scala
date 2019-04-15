package org.simoexpo.as4k.producer

import org.simoexpo.as4k.testing.BaseSpec

class KafkaProducerOptionSpec extends BaseSpec {

  "KafkaProducerOption" should {

    "create a KafkaProducer with the correct setting" in {

      val kafkaProducerOption = KafkaProducerOption("my-simple-producer")

      val props = kafkaProducerOption.producerSetting

      noException should be thrownBy kafkaProducerOption.createOne()

      kafkaProducerOption.isTransactional shouldBe false
      props("bootstrap.servers") shouldBe "127.0.0.1:9092"
      props("acks") shouldBe "all"
      props("batch.size") shouldBe "16384"
      props("linger.ms") shouldBe "1"
      props("buffer.memory") shouldBe "33554432"
      props("key.serializer") shouldBe "org.apache.kafka.common.serialization.StringSerializer"
      props("value.serializer") shouldBe "org.apache.kafka.common.serialization.StringSerializer"
    }

    "create a KafkaProducer with the correct setting if transactional" in {

      val kafkaProducerOption = KafkaProducerOption("my-transactional-producer")

      val props = kafkaProducerOption.producerSetting

      noException should be thrownBy kafkaProducerOption.createOne()

      kafkaProducerOption.isTransactional shouldBe true
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

      an[Exception] should be thrownBy KafkaProducerOption("no-setting")

    }

  }
}
