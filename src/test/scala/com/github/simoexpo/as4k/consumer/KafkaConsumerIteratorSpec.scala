package com.github.simoexpo.as4k.consumer

import com.github.simoexpo.BaseSpec
import com.github.simoexpo.as4k.consumer.KafkaConsumerActor.ConsumerToken

class KafkaConsumerIteratorSpec extends BaseSpec {

  "KafkaConsumerIteratorSpec" should {

    val iterator = KafkaConsumerIterator.getKafkaIterator

    "always have another elements" in {
      (1 to 10000).foreach { _ =>
        iterator().hasNext shouldBe true
      }
    }

    "return a ConsumerToken" in {
      iterator().next() shouldBe ConsumerToken
    }

  }

}
