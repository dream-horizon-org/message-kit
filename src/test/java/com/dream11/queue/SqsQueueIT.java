package com.dream11.queue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static software.amazon.awssdk.services.sqs.model.QueueAttributeName.VISIBILITY_TIMEOUT;

import com.dream11.queue.config.HeartbeatConfig;
import com.dream11.queue.impl.sqs.SqsConfig;
import com.dream11.queue.impl.sqs.SqsConsumer;
import com.dream11.queue.impl.sqs.SqsProducer;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import lombok.SneakyThrows;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesRequest;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;

@ExtendWith({Setup.class})
class SqsQueueIT {

  private static SqsConsumer SQS_CONSUMER;
  private static SqsProducer<String> SQS_PRODUCER;

  private static SqsAsyncClient SQS_ASYNC_CLIENT;

  private static String QUEUE_URL;
  private static SqsConfig.SqsConfigBuilder SQS_CONFIG_BUILDER;

  @BeforeAll
  public static void setup() throws ExecutionException, InterruptedException {
    SQS_ASYNC_CLIENT =
        SqsAsyncClient.builder()
            .endpointOverride(URI.create(System.getProperty(Constants.SQS_ENDPOINT)))
            .region(Region.of(System.getProperty(Constants.AWS_REGION)))
            .build();

    CreateQueueRequest createQueueRequest =
        CreateQueueRequest.builder()
            .queueName(Constants.SQS_QUEUE)
            .attributes(Map.of(VISIBILITY_TIMEOUT, "1"))
            .build();

    QUEUE_URL = SQS_ASYNC_CLIENT.createQueue(createQueueRequest).get().queueUrl();

    SQS_CONFIG_BUILDER =
        SqsConfig.builder()
            .queueUrl(QUEUE_URL)
            .region(System.getProperty(Constants.AWS_REGION))
            .endpoint(System.getProperty(Constants.SQS_ENDPOINT))
            .receiveConfig(SqsConfig.ReceiveConfig.builder().maxMessages(2).build());

    SQS_CONSUMER = new SqsConsumer(SQS_CONFIG_BUILDER.build());
    SQS_PRODUCER = new SqsProducer<>(SQS_CONFIG_BUILDER.build());
  }

  @AfterAll
  public static void tearDown() {
    if (SQS_PRODUCER != null) {
      SQS_PRODUCER.close();
    }
    if (SQS_CONSUMER != null) {
      SQS_CONSUMER.close();
    }
    if (SQS_ASYNC_CLIENT != null) {
      SQS_ASYNC_CLIENT.close();
    }
  }

  @Test
  @SneakyThrows
  void testSendAndReceive() {
    // Arrange
    String message1 = "test message 1";
    String message2 = "test message 2";
    String message3 = "test message 3";

    // Act
    SQS_PRODUCER.send(message1).get();
    SQS_PRODUCER.send(message2).get();
    SQS_PRODUCER.send(message3).get();
    List<Message> firstMessages = SQS_CONSUMER.receive().get();
    SQS_CONSUMER.acknowledgeMessage(firstMessages.get(0)).get();
    SQS_CONSUMER.acknowledgeMessage(firstMessages.get(1)).get();
    List<Message> secondMessages = SQS_CONSUMER.receive().get();
    SQS_CONSUMER.acknowledgeMessage(secondMessages.get(0)).get();

    // Assert
    assertThat(firstMessages).hasSize(2);
    assertThat(firstMessages.get(0).getBody()).isEqualTo(message1);
    assertThat(firstMessages.get(1).getBody()).isEqualTo(message2);
    assertThat(secondMessages).hasSize(1);
    assertThat(secondMessages.get(0).getBody()).isEqualTo(message3);
  }

  @Test
  @SneakyThrows
  void testSendAndReceiveWithTransformer() {
    // Arrange
    String message = "test message";
    SqsProducer<String> producer =
        new SqsProducer<>(SQS_CONFIG_BUILDER.build(), String::toUpperCase);
    // Act
    producer.send(message).get();
    List<Message> messages = SQS_CONSUMER.receive().get();
    SQS_CONSUMER.acknowledgeMessage(messages.get(0)).get();

    // Assert
    assertThat(messages).hasSize(1);
    assertThat(messages.get(0).getBody()).isEqualTo(message.toUpperCase());
  }

  @Test
  @SneakyThrows
  void testSendAndReceiveWithTimeout() {
    // Arrange
    String message = "test message with timeout";

    // Act
    SQS_PRODUCER.send(message).get();
    List<Message> messages = SQS_CONSUMER.receive(2).get();
    SQS_CONSUMER.acknowledgeMessage(messages.get(0)).get();

    // Assert
    assertThat(messages).hasSize(1);
    assertThat(messages.get(0).getBody()).isEqualTo(message);
  }

  //
  @Test
  @SneakyThrows
  void testSendAndReceiveNoMessageAvailable() {
    // Arrange & Act
    List<Message> messages = SQS_CONSUMER.receive().get();

    // Assert
    assertThat(messages).isEmpty();
  }

  //
  @Test
  @SneakyThrows
  void testSendAndReceiveWithTimeoutNoMessageAvailable() {
    // Arrange & Act
    List<Message> messages = SQS_CONSUMER.receive(2).get();

    // Act
    assertThat(messages).isEmpty();
  }

  @Test
  @SneakyThrows
  void testAcknowledgeMessage() {
    // Arrange
    String message = "test ack message";

    // Act
    SQS_PRODUCER.send(message).get();
    List<Message> messages = SQS_CONSUMER.receive().get();
    SQS_CONSUMER.acknowledgeMessage(messages.get(0)).get();

    // Assert
    assertThat(messages).hasSize(1);
    assertThat(messages.get(0).getBody()).isEqualTo(message);
    assertThat(
            SQS_ASYNC_CLIENT
                .getQueueAttributes(
                    GetQueueAttributesRequest.builder()
                        .queueUrl(QUEUE_URL)
                        .attributeNames(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES)
                        .build())
                .get()
                .attributes())
        .containsEntry(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES, "0");
  }

  @Test
  @SneakyThrows
  void testAndReceiveWithHeartbeat() {
    // Arrange
    String message = "test message";
    SqsConfig sqsConfig =
        SQS_CONFIG_BUILDER
            .heartbeatConfig(HeartbeatConfig.builder().heartbeatInterval(1).build())
            .build();

    SqsConsumer sqsConsumer = new SqsConsumer(sqsConfig);
    SqsProducer<String> producer = new SqsProducer<>(sqsConfig);

    // Act & Assert
    producer.send(message).get();
    List<Message> messages = sqsConsumer.receive().get();
    assertThat(messages).hasSize(1);
    assertThat(messages.get(0).getBody()).isEqualTo(message);

    // Wait for heartbeat to be sent
    await()
        .pollDelay(Duration.ofSeconds(2))
        .untilAsserted(
            () -> {
              List<Message> newMessages = sqsConsumer.receive().get();
              assertThat(newMessages).isEmpty();
              sqsConsumer.acknowledgeMessage(messages.get(0)).get();
            });
  }

  @Test
  @SneakyThrows
  void testAndReceiveWithoutHeartbeat() {
    // Arrange
    String message = "test message";

    // Act & Assert
    SQS_PRODUCER.send(message).get();
    List<Message> messages = SQS_CONSUMER.receive().get();
    assertThat(messages).hasSize(1);
    assertThat(messages.get(0).getBody()).isEqualTo(message);

    // Wait for message to visible again
    await()
        .pollDelay(Duration.ofSeconds(2))
        .untilAsserted(
            () -> {
              List<Message> newMessages = SQS_CONSUMER.receive().get();
              assertThat(newMessages).hasSize(1);
              assertThat(newMessages.get(0).getBody()).isEqualTo(message);
              SQS_CONSUMER.acknowledgeMessage(newMessages.get(0)).get();
            });
  }
}
