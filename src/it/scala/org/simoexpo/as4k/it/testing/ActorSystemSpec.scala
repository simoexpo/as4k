package org.simoexpo.as4k.it.testing

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.testkit.TestKit
import akka.util.Timeout
import org.scalatest.{BeforeAndAfterAll, WordSpec}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

trait ActorSystemSpec extends WordSpec with BeforeAndAfterAll {

  implicit val system: ActorSystem = ActorSystem("test")
  implicit val ec: ExecutionContext = system.dispatchers.lookup("consumer-dispatcher-2")
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val timeout: Timeout = Timeout(FiniteDuration(5, "seconds"))

  override def afterAll(): Unit = {
    super.afterAll()
    TestKit.shutdownActorSystem(system, 10.seconds)
  }

}
