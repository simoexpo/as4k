package com.github.simoexpo.as4k.consumer

import com.github.simoexpo.BaseSpec
import org.apache.kafka.common.serialization.StringDeserializer

class KafkaConsumerOptionSpec extends BaseSpec {

  "KafkaConsumerOption" should {

    "create a KafkaConsumer with the correct setting" in {

      val kafkaConsumerOption = KafkaConsumerOption(
        clientId = "clientId",
        groupId = "groupId",
        topics = List("topic"),
        bootstrapServers = "127.0.0.1:9092",
        enableAutoCommit = true,
        autoCommitIntervalMs = Some(100),
        keyDeserializer = new StringDeserializer,
        valueDeserializer = new StringDeserializer
      )

//      val kafkaConsumer = kafkaConsumerOption.createOne()

    }

  }

}
