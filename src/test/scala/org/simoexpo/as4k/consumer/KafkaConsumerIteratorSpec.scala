package org.simoexpo.as4k.consumer

import org.simoexpo.as4k.consumer.KafkaConsumerActor.ConsumerToken
import org.simoexpo.as4k.testing.BaseSpec

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
