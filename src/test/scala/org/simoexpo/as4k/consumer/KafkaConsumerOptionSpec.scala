package org.simoexpo.as4k.consumer

import org.simoexpo.as4k.testing.BaseSpec

class KafkaConsumerOptionSpec extends BaseSpec {

  "KafkaConsumerOption" should {

    val topics = Seq("topic1", "topic2")

    "create a KafkaConsumer with the correct setting" in {

      val kafkaConsumerOption = KafkaConsumerOption(topics, "my-consumer")

      val props = kafkaConsumerOption.consumerSetting

      noException should be thrownBy kafkaConsumerOption.createOne()

      kafkaConsumerOption.groupId shouldBe Some("test")
      kafkaConsumerOption.topics shouldBe topics
      props("client.id") shouldBe "integrationTest"
      props("bootstrap.servers") shouldBe "127.0.0.1:9092"
      props("group.id") shouldBe "test"
      props("auto.offset.reset") shouldBe "earliest"
      props("enable.auto.commit") shouldBe "true"
      props("auto.commit.interval.ms") shouldBe "50"
      props("key.deserializer") shouldBe "org.apache.kafka.common.serialization.StringDeserializer"
      props("value.deserializer") shouldBe "org.apache.kafka.common.serialization.StringDeserializer"
    }

    "fail to create KafkaConsumerOption if no consumer setting are found" in {

      an[Exception] should be thrownBy KafkaConsumerOption(topics, "no-setting")

    }

  }

}
