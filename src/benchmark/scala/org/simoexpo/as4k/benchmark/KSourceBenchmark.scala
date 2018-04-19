package org.simoexpo.as4k.benchmark

import akka.stream.scaladsl.{Sink, Source}
import org.simoexpo.as4k.KSource
import org.simoexpo.as4k.consumer.{KafkaConsumerAgent, KafkaConsumerOption}
import org.simoexpo.as4k.model.{KRecord, KRecordMetadata}
import org.simoexpo.as4k.producer.{KafkaProducerOption, KafkaSimpleProducerAgent, KafkaTransactionalProducerAgent}
import org.simoexpo.as4k.KSource._

import scala.concurrent.Future
import scala.util.control.NonFatal

object KSourceBenchmark extends App with ActorSystemUtility with KafkaManagerUtility {

  val inTopic = "in_topic"
  val outTopic = "out_topic"

  val recordsSize = 1000000

  val kRecords = Stream.from(0).map(n => aKRecord(n, n.toString, s"value$n", "topic", 1, "defaultGroup"))

  val benchmark = for {
    _ <- setUpBenchmarkEnv
    _ <- simpleConsumerBenchmark
    _ <- simpleConsumerWithCommitBenchmark
    _ <- simpleConsumerWithBatchCommitBenchmark
    _ <- simpleConsumerWithSimpleProducerBenchmark
    _ <- simpleConsumerWithTransactionalProducerBenchmark
    _ <- simpleConsumerWithTransactionalProducerWithCommitBenchmark
  } yield ()

  benchmark.recover {
    case NonFatal(ex) => Console.println(s"Failed to execute benchmark: $ex")
  }.andThen {
    case _ => system.terminate()
  }

  private def setUpBenchmarkEnv: Future[Unit] = {
    val kafkaSimpleProducerOption: KafkaProducerOption[String, String] = KafkaProducerOption(inTopic, "my-simple-producer")
    Console.println("Creating topics...")
    for {
      _ <- createSimpleTopics(Seq(inTopic, outTopic))
      _ = Console.println("Start producing...")
      kafkaProducerAgent = new KafkaSimpleProducerAgent(kafkaSimpleProducerOption)
      _ <- Source.fromIterator(() => kRecords.iterator).take(recordsSize).produce(100)(kafkaProducerAgent).runWith(Sink.ignore)
      _ = Console.println("Finished producing messages")
      _ = Console.println("Clean up resources")
      _ <- kafkaProducerAgent.stopProducer
    } yield ()
  }

  private def simpleConsumerBenchmark: Future[Unit] = {
    val benchName = "Simple Consumer"
    val kafkaConsumerOption: KafkaConsumerOption[String, String] = KafkaConsumerOption(Seq(inTopic), "my-simple-consumer")
    val kafkaConsumerAgent = new KafkaConsumerAgent(kafkaConsumerOption, 100)
    val bench = KSource.fromKafkaConsumer(kafkaConsumerAgent).take(recordsSize)
    Console.println(s"Start $benchName Benchmark...")
    val start = System.currentTimeMillis()
    for {
      _ <- bench.runWith(Sink.ignore)
      elapsedTime = System.currentTimeMillis() - start
      _ = printBenchTimeResult(benchName, elapsedTime)
      _ = printBenchMsgPerSecResult(benchName, recordsSize, elapsedTime)
      _ = Console.println("Clean up resources")
      _ <- kafkaConsumerAgent.stopConsumer
    } yield ()
  }

  private def simpleConsumerWithCommitBenchmark: Future[Unit] = {
    val benchName = "Simple Consumer with Commit"
    val kafkaConsumerOption: KafkaConsumerOption[String, String] =
      KafkaConsumerOption(Seq(inTopic), "my-simple-consumer-with-commit")
    val kafkaConsumerAgent = new KafkaConsumerAgent(kafkaConsumerOption, 100)
    val bench = KSource.fromKafkaConsumer(kafkaConsumerAgent).commit(3)(kafkaConsumerAgent).take(recordsSize)
    Console.println(s"Start $benchName Benchmark...")
    val start = System.currentTimeMillis()
    for {
      _ <- bench.runWith(Sink.ignore)
      elapsedTime = System.currentTimeMillis() - start
      _ = printBenchTimeResult(benchName, elapsedTime)
      _ = printBenchMsgPerSecResult(benchName, recordsSize, elapsedTime)
      _ = Console.println("Clean up resources")
      _ <- kafkaConsumerAgent.stopConsumer
    } yield ()
  }

  private def simpleConsumerWithBatchCommitBenchmark: Future[Unit] = {
    val benchName = "Simple Consumer with Batch Commit"
    val kafkaConsumerOption: KafkaConsumerOption[String, String] =
      KafkaConsumerOption(Seq(inTopic), "my-simple-consumer-with-batch-commit")
    val kafkaConsumerAgent = new KafkaConsumerAgent(kafkaConsumerOption, 100)
    val batchSize = 1000
    val bench =
      KSource.fromKafkaConsumer(kafkaConsumerAgent).grouped(batchSize).commit(3)(kafkaConsumerAgent).take(recordsSize / batchSize)
    Console.println(s"Start $benchName Benchmark...")
    val start = System.currentTimeMillis()
    for {
      _ <- bench.runWith(Sink.ignore)
      elapsedTime = System.currentTimeMillis() - start
      _ = printBenchTimeResult(benchName, elapsedTime)
      _ = printBenchMsgPerSecResult(benchName, recordsSize, elapsedTime)
      _ = Console.println("Clean up resources")
      _ <- kafkaConsumerAgent.stopConsumer
    } yield ()
  }

  private def simpleConsumerWithSimpleProducerBenchmark: Future[Unit] = {
    val benchName = "Simple Consumer / Simple Producer"
    val kafkaConsumerOption: KafkaConsumerOption[String, String] =
      KafkaConsumerOption(Seq(inTopic), "my-simple-consumer-two")
    val kafkaSimpleProducerOption: KafkaProducerOption[String, String] = KafkaProducerOption(outTopic, "my-simple-producer")
    val kafkaConsumerAgent = new KafkaConsumerAgent(kafkaConsumerOption, 100)
    val kafkaProducerAgent = new KafkaSimpleProducerAgent(kafkaSimpleProducerOption)
    val bench =
      KSource.fromKafkaConsumer(kafkaConsumerAgent).produce(100)(kafkaProducerAgent).take(recordsSize)
    Console.println(s"Start $benchName Benchmark...")
    val start = System.currentTimeMillis()
    for {
      _ <- bench.runWith(Sink.ignore)
      elapsedTime = System.currentTimeMillis() - start
      _ = printBenchTimeResult(benchName, elapsedTime)
      _ = printBenchMsgPerSecResult(benchName, recordsSize, elapsedTime)
      _ = Console.println("Clean up resources")
      _ <- kafkaConsumerAgent.stopConsumer
      _ <- kafkaProducerAgent.stopProducer
    } yield ()
  }

  private def simpleConsumerWithTransactionalProducerBenchmark: Future[Unit] = {
    val benchName = "Simple Consumer / Transactional Producer"
    val kafkaConsumerOption: KafkaConsumerOption[String, String] =
      KafkaConsumerOption(Seq(inTopic), "my-simple-consumer-three")
    val kafkaTransactionalProducerOption: KafkaProducerOption[String, String] =
      KafkaProducerOption(outTopic, "my-transactional-producer")
    val batchSize = 100
    val kafkaConsumerAgent = new KafkaConsumerAgent(kafkaConsumerOption, 100)
    val kafkaProducerAgent = new KafkaTransactionalProducerAgent(kafkaTransactionalProducerOption)
    val bench =
      KSource.fromKafkaConsumer(kafkaConsumerAgent).grouped(batchSize).produce(kafkaProducerAgent).take(recordsSize / batchSize)
    Console.println(s"Start $benchName Benchmark...")
    val start = System.currentTimeMillis()
    for {
      _ <- bench.runWith(Sink.ignore)
      elapsedTime = System.currentTimeMillis() - start
      _ = printBenchTimeResult(benchName, elapsedTime)
      _ = printBenchMsgPerSecResult(benchName, recordsSize, elapsedTime)
      _ = Console.println("Clean up resources")
      _ <- kafkaConsumerAgent.stopConsumer
      _ <- kafkaProducerAgent.stopProducer
    } yield ()
  }

  private def simpleConsumerWithTransactionalProducerWithCommitBenchmark: Future[Unit] = {
    val benchName = "Simple Consumer / Transactional Producer with Commit"
    val kafkaConsumerOption: KafkaConsumerOption[String, String] =
      KafkaConsumerOption(Seq(inTopic), "my-simple-consumer-four")
    val kafkaTransactionalProducerOption: KafkaProducerOption[String, String] =
      KafkaProducerOption(outTopic, "my-transactional-producer")
    val batchSize = 100
    val kafkaConsumerAgent = new KafkaConsumerAgent(kafkaConsumerOption, 100)
    val kafkaProducerAgent = new KafkaTransactionalProducerAgent(kafkaTransactionalProducerOption)
    val bench =
      KSource
        .fromKafkaConsumer(kafkaConsumerAgent)
        .grouped(batchSize)
        .produceAndCommit(kafkaProducerAgent)
        .take(recordsSize / batchSize)
    Console.println(s"Start $benchName Benchmark...")
    val start = System.currentTimeMillis()
    for {
      _ <- bench.runWith(Sink.ignore)
      elapsedTime = System.currentTimeMillis() - start
      _ = printBenchTimeResult(benchName, elapsedTime)
      _ = printBenchMsgPerSecResult(benchName, recordsSize, elapsedTime)
      _ = Console.println("Clean up resources")
      _ <- kafkaConsumerAgent.stopConsumer
      _ <- kafkaProducerAgent.stopProducer
    } yield ()
  }

  private def printBenchTimeResult(benchName: String, time: Long): Unit =
    println(Console.GREEN + s"$benchName Benchmark - total time: $time ms" + Console.RESET)

  private def printBenchMsgPerSecResult(benchName: String, recordSize: Int, time: Long): Unit =
    println(Console.GREEN + s"$benchName Benchmark - msg/s: ${recordsSize / time.toDouble * 1000}" + Console.RESET)

  def aKRecord[K, V](offset: Long, key: K, value: V, topic: String, partition: Int, consumedBy: String): KRecord[K, V] = {
    val metadata = KRecordMetadata(topic, partition, offset, System.currentTimeMillis(), consumedBy)
    KRecord(key, value, metadata)
  }

}
