# Message Kit
[![codecov](https://codecov.io/gh/dream-horizon-org/message-kit/graph/badge.svg?token=PANVES8PXA)](https://codecov.io/gh/ds-horizon/message-kit)

A lightweight Java library for working with message queues, currently supporting Amazon SQS.

## Features

- Simple and intuitive API for message producers and consumers
- Support for Amazon SQS
- Asynchronous message processing
- Heartbeat mechanism for long-running message processing

## Installation

Add the following dependency to your `pom.xml`:

```xml
<dependency>
    <groupId>com.dream11</groupId>
    <artifactId>message-kit</artifactId>
    <version>x.y.z</version>
</dependency>
```

## Usage

### Configuration

#### SQS

```java
Config config = SqsConfig.builder()
    .queueUrl("https://sqs.region.amazonaws.com/queue-name")
    .region("us-east-1")
    .build();
```

### Producing Messages

```java
// Create a message producer
MessageProducer<String> producer = MessageProducerFactory.create(config);

// Send a message
CompletableFuture<Void> future = producer.send("Hello, World!");

// Don't forget to close the producer when done
producer.close();
```

### Consuming Messages

```java
// Create a message consumer
MessageConsumer consumer = MessageConsumerFactory.create(config);

// Receive messages
CompletableFuture<List<Message>> messages = consumer.receive();

// Process messages
messages.thenAccept(messageList -> {
    for (Message message : messageList) {
        // Process message
        consumer.acknowledgeMessage(message);
    }
});

// Don't forget to close the consumer when done
consumer.close();
```

### Heartbeat Mechanism

For long-running message processing, use the heartbeat mechanism to prevent message visibility timeout:

```java
// Create a config with heartbeat enabled
Config config = SqsConfig.builder()
    .queueUrl("https://sqs.region.amazonaws.com/queue-name")
    .region("us-east-1")
    .heartbeatConfig(HeartbeatConfig.builder().heartbeatInterval(1).build())
    .build();
```

## Configuration Options

### SQS Configuration

| Option | Description | Required | Default |
|--------|-------------|----------|---------|
| queueUrl | The URL of the SQS queue | Yes | - |
| region | The AWS region where the SQS queue is located | Yes | - |
| endpoint | Custom endpoint for the SQS queue | No | - |
| receiveConfig | Configuration for receiving messages | No | maxMessages=1 |

#### Receive Configuration

| Option | Description | Required | Default |
|--------|-------------|----------|---------|
| maxMessages | Maximum number of messages to receive in one batch | No | 1 |

#### Heartbeat Configuration

| Option | Description | Required | Default |
|--------|-------------|----------|---------|
| heartbeatInterval | Interval in seconds between heartbeats. Set to -1 to disable heartbeats | No | -1 |
| executorThreadPoolSize | Number of threads in the executor pool for sending heartbeats | No | 2 |
