package com.github.simoexpo.as4k.factory

import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{Matchers, WordSpec}

import scala.collection.JavaConverters._

class CallbackFactorySpec extends WordSpec with Matchers with MockitoSugar {

  "CallbackFactory" should {

    "create an OffsetCommitCallback with the defined behavior" in {

      val callback = CallbackFactory { (offset: Map[TopicPartition, OffsetAndMetadata], exception: Option[Exception]) =>
        exception match {
          case None     => println(s"successfully commit offset $offset")
          case Some(ex) => throw ex
        }
      }

      val offset: Map[TopicPartition, OffsetAndMetadata] = Map.empty

      noException should be thrownBy callback.onComplete(offset.asJava, null)

      an[IllegalStateException] should be thrownBy callback.onComplete(offset.asJava, new IllegalStateException())

    }

  }

}
