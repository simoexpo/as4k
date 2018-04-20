# as4k [![Build Status](https://travis-ci.org/simoexpo/as4k.svg?branch=master)](https://travis-ci.org/simoexpo/as4k?branch=master) [![Coverage Status](https://coveralls.io/repos/github/simoexpo/as4k/badge.svg?branch=master)](https://coveralls.io/github/simoexpo/as4k?branch=master)

*A simple Akka Stream extension for Kafka*

```scala
KSource
  .fromKafkaConsumer(kafkaConsumerAgent)
  .mapValue(_.toString)
  .runWith(KSink.produce(kafkaProducerAgent))
```

## What is it?

as4k is a simple Akka Stream extension that aims to simplify the integration with Kafka to create streams from a consumer or produce stream to a topic.

## How does it works?

as4k provides some simple classes to consume messages from a Kafka topic (`KafkaConsumerAgent`) and to produce messages to a topic (`KafkaSimpleProducerAgent` and `KafkaTransactionalProducerAgent`).

In addition it provides some utilities to create streams `Source` from `KafkaConsumerAgent` and streams `Sink` from a `KafkaSimpleProducerAgent` (or `KafkaTransactionalProducerAgent`)

### KafkaConsumerAgent

In progress...

### KafkaProducerAgents

In progress...

### KSource

In progress...

### KSource

In progress...

## Examples

In progress...

```scala
import org.simoexpo.as4k.KSource
import org.simoexpo.as4k.KSource._
import org.simoexpo.as4k.KSink
import org.simoexpo.as4k.KSink._
import org.simoexpo.as4k.consumer.KafkaConsumerAgent
import org.simoexpo.as4k.producer.KafkaSimpleProducerAgent

val kafkaConsumerAgent: KafkaConsumerAgent[Long, Long] = ???
val kafkaProducerAgent: KafkaSimpleProducerAgent[Long, String] = ???

KSource
  .fromKafkaConsumer(kafkaConsumerAgent)
  .mapValue(_.toString)
  .runWith(KSink.produce(kafkaProducerAgent))
```
