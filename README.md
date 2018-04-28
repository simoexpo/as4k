# as4k [![Build Status](https://travis-ci.org/simoexpo/as4k.svg?branch=master)](https://travis-ci.org/simoexpo/as4k?branch=master) [![Coverage Status](https://coveralls.io/repos/github/simoexpo/as4k/badge.svg?branch=master)](https://coveralls.io/github/simoexpo/as4k?branch=master) [![Codacy Badge](https://api.codacy.com/project/badge/Grade/0133d3b34661447d9f6b4b38983ee5e7)](https://www.codacy.com/app/simoexpo/as4k?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=simoexpo/as4k&amp;utm_campaign=Badge_Grade) [![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://raw.githubusercontent.com/simoexpo/as4k/master/LICENSE.txt)

*A simple Akka Stream extension for Kafka integration*

```scala
KSource
  .fromKafkaConsumer(kafkaConsumerAgent)
  .mapValue(value => f(value))
  .runWith(KSink.produce(kafkaProducerAgent))
```

## What is it?

as4k is a simple Akka Stream extension that aims to simplify the integration with Kafka to create streams from a consumer or produce stream to a topic.

## How does it works?

as4k provides some simple classes to consume messages from a Kafka topic (`KafkaConsumerAgent`) and to produce messages to a topic (`KafkaSimpleProducerAgent` and `KafkaTransactionalProducerAgent`).

In addition it provides some utilities to create Akka Streams `Source` from `KafkaConsumerAgent` and `Sink` from a `KafkaSimpleProducerAgent` (or `KafkaTransactionalProducerAgent`)

### KafkaConsumerAgent

A `KafkaConsumerAgent` wrap a Kafka consumer and allow to perform the following operation:
+ Polling for new messages
+ Committing a message or a sequence of messages

To create a `KafkaConsumerAgent` you first need to define its behaviour with a `KafkaConsumerOption` that allow to specify:
+ the list of topics to subscribe
+ the polling timeout
+ a complete set of consumer configuration as defined in the official [Kafka documentation](https://kafka.apache.org/documentation/#consumerconfigs)
+ the dispatcher used by the `KafkaConsumerAgent` (optional)
+ key and value deserializer (optional, override config setting)

NOTE: the config name should be separated by `-` NOT `.`

### KafkaProducerAgents

KafkaProducerAgent comes in two flavour: `KafkaSimpleProducerAgent` and `KafkaTransactionalProducerAgent`.
The first one represent a normal Kafka producer and allow to produce a single messages on a topic.
The second one, instead, represent a Kafka transactional producer and expose methods to:
+ Produce a sequence of messages in transaction
+ Produce and commit in transaction a single message or a sequence of messages

To create a KafkaProducerAgent you first need to define its behaviour with a `KafkaProducerOption` that allow to specify:
+ the topic to produce
+ a complete set of producer configuration as defined in the official [Kafka documentation](https://kafka.apache.org/documentation/#producerconfigs)
+ the dispatcher used by the KafkaProducerAgent (optional)
+ key and value serializer (optional, override config setting)

NOTE1: the config name should be separated by dash (`-`) NOT dot (`.`)  
NOTE2: To define a `KafkaTransactionalProducerAgent` the property `transactional-id` is mandatory.

### KSource

`KSource` allow to create an Akka Source from a `KafkaConsumerAgent`.
The resulting source will be a `Source[KRecord[K,V], Any]` where KRecord represent a message with key of type `K` and value of type `V`.

In addition it extends the common methods of Akka Source and add some utility to transform and work with a stream of `KRecord` (or `Seq[KRecord]`):
+ commit
+ map value of the message without changing the key
+ produce messages
+ produce and commit messages in transaction (exactly once semantic)

### KSink

`KSink` provide some Akka Sink to work with `Krecord` (or `Seq[KRecord]`) stream, in particular:
+ A producer Sink
+ A produce and commit in transaction Sink (exactly once semantic)

## Examples

application.conf
```
my-consumer {

  consumer-setting {
    client-id = "as4k"
    bootstrap-servers = "kafkabroker:9092"
    group-id = "as4k_example"
    auto-offset-reset = "earliest"
    enable-auto-commit = false
    key-deserializer = "org.apache.kafka.common.serialization.StringDeserializer"
    value-deserializer = "org.apache.kafka.common.serialization.LongDeserializer"
  }

}

my-simple-producer {

  producer-setting {
    bootstrap-servers = "kafkabroker:9092"
    acks = "all"
    batch-size = 16384
    linger-ms = 1
    buffer-memory = 33554432
    key-serializer = "org.apache.kafka.common.serialization.StringSerializer"
    value-serializer = "org.apache.kafka.common.serialization.StringSerializer"
  }

}
```
Scala code:
```scala
import org.simoexpo.as4k.{KSource, KSink}
import org.simoexpo.as4k.KSource._
import org.simoexpo.as4k.consumer.{KafkaConsumerAgent, KafkaConsumerOption}
import org.simoexpo.as4k.producer.{KafkaSimpleProducerAgent, KafkaProducerOption}

val kafkaConsumerOption: KafkaConsumerOption[String, Long] = KafkaConsumerOption(Seq("in-topic"), "my-consumer")
val kafkaSimpleProducerOption: KafkaProducerOption[String, String] = KafkaProducerOption("out-topic", "my-simple-producer")

val kafkaConsumerAgent: KafkaConsumerAgent[String, Long] = new KafkaConsumerAgent(kafkaConsumerOption)
val kafkaProducerAgent: KafkaSimpleProducerAgent[String, String] = new KafkaSimpleProducerAgent(kafkaSimpleProducerOption)

def f: Long => String = ???

KSource
  .fromKafkaConsumer(kafkaConsumerAgent)
  .commit()(kafkaConsumerAgent)
  .mapValue(value => f(value))
  .runWith(KSink.produce(kafkaProducerAgent))
```
