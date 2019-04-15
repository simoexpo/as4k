package org.simoexpo.as4k.it.testing

import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}

trait BaseSpec extends WordSpec with Matchers with MockitoSugar with BeforeAndAfterAll {

  implicit val config: EmbeddedKafkaConfig = EmbeddedKafkaConfig(kafkaPort = 9092, zooKeeperPort = 2181)

  override def beforeAll(): Unit =
    EmbeddedKafka.start()

  override def afterAll(): Unit =
    EmbeddedKafka.stop()

}
