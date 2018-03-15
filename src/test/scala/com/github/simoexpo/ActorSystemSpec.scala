package com.github.simoexpo

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.testkit.TestKit
import akka.util.Timeout
import org.scalatest.{BeforeAndAfterAll, WordSpec}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration

trait ActorSystemSpec extends WordSpec with BeforeAndAfterAll {

  implicit val system: ActorSystem = ActorSystem("test")
  implicit val ec: ExecutionContext = system.dispatcher
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val timeout: Timeout = Timeout(FiniteDuration(1, "seconds"))

  override def afterAll(): Unit = {
    super.afterAll()
    TestKit.shutdownActorSystem(system)
  }

}
