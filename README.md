# as4k [![Build Status](https://travis-ci.org/simoexpo/as4k.svg?branch=master)](https://travis-ci.org/simoexpo/as4k?branch=master) [![Coverage Status](https://coveralls.io/repos/github/simoexpo/as4k/badge.svg?branch=master)](https://coveralls.io/github/simoexpo/as4k?branch=master) [![Codacy Badge](https://api.codacy.com/project/badge/Grade/0133d3b34661447d9f6b4b38983ee5e7)](https://www.codacy.com/app/simoexpo/as4k?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=simoexpo/as4k&amp;utm_campaign=Badge_Grade) [![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://raw.githubusercontent.com/simoexpo/as4k/master/LICENSE.txt)

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

A `KafkaConsumerAgent` wrap a Kafka consumer and allow to perform the following operation:
+ Polling for new messages
+ Committing a message or a sequence of messages

In progress...

### KafkaProducerAgents

KafkaProducerAgent comes in two flavour: `KafkaSimpleProducerAgent` and `KafkaTransactionalProducerAgent`.
The first one represent a normal Kafka producer and allow to produce a single messages on a topic.
The second one, instead, represent a Kafka transactional producer and expose methods to:
+ Produce a sequence of messages in transaction
+ Produce and commit in transaction a single message or a sequence of messages

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
