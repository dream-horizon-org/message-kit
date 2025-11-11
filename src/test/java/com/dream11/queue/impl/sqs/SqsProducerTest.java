package com.dream11.queue.impl.sqs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;

class SqsProducerTest {

  private SqsConfig sqsConfig;
  private SqsAsyncClient mockSqsAsyncClient;

  @BeforeEach
  void setUp() {
    sqsConfig = SqsConfig.builder().region("us-east-1").queueUrl("test-queue-url").build();
    mockSqsAsyncClient = mock(SqsAsyncClient.class);
    when(mockSqsAsyncClient.sendMessage(any(SendMessageRequest.class)))
        .thenReturn(CompletableFuture.completedFuture(SendMessageResponse.builder().build()));
  }

  @Test
  void testSupportsMessageAttributesReturnsTrue() {
    // Arrange
    SqsProducer<String> producer = new SqsProducer<>(sqsConfig, mockSqsAsyncClient);

    // Act
    boolean supportsAttributes = producer.supportsMessageAttributes();

    // Assert
    assertThat(supportsAttributes).isTrue();
  }

  @Test
  void testSendWithoutAttributes() throws Exception {
    // Arrange
    SqsProducer<String> producer = new SqsProducer<>(sqsConfig, mockSqsAsyncClient);
    String message = "test message";

    // Act
    producer.send(message).get();

    // Assert
    verify(mockSqsAsyncClient).sendMessage(any(SendMessageRequest.class));
  }

  @Test
  void testSendWithAttributes() throws Exception {
    // Arrange
    SqsProducer<String> producer = new SqsProducer<>(sqsConfig, mockSqsAsyncClient);
    String message = "test message";
    Map<String, Object> attributes = new HashMap<>();
    attributes.put("key1", "value1");
    attributes.put("key2", 123);

    // Act
    producer.send(message, attributes).get();

    // Assert
    verify(mockSqsAsyncClient).sendMessage(any(SendMessageRequest.class));
  }

  @Test
  void testSendWithEmptyAttributes() throws Exception {
    // Arrange
    SqsProducer<String> producer = new SqsProducer<>(sqsConfig, mockSqsAsyncClient);
    String message = "test message";
    Map<String, Object> attributes = new HashMap<>();

    // Act
    producer.send(message, attributes).get();

    // Assert
    verify(mockSqsAsyncClient).sendMessage(any(SendMessageRequest.class));
  }

  @Test
  void testSendWithNullAttributes() throws Exception {
    // Arrange
    SqsProducer<String> producer = new SqsProducer<>(sqsConfig, mockSqsAsyncClient);
    String message = "test message";

    // Act
    producer.send(message, Map.of()).get();

    // Assert
    verify(mockSqsAsyncClient).sendMessage(any(SendMessageRequest.class));
  }

  @Test
  void testSendWithTransformer() throws Exception {
    // Arrange
    SqsProducer<String> producer =
        new SqsProducer<>(sqsConfig, mockSqsAsyncClient, String::toUpperCase);
    String message = "test message";

    // Act
    producer.send(message).get();

    // Assert
    verify(mockSqsAsyncClient).sendMessage(any(SendMessageRequest.class));
  }

  @Test
  void testSendWithTransformerAndAttributes() throws Exception {
    // Arrange
    SqsProducer<String> producer =
        new SqsProducer<>(sqsConfig, mockSqsAsyncClient, String::toUpperCase);
    String message = "test message";
    Map<String, Object> attributes = new HashMap<>();
    attributes.put("key1", "value1");

    // Act
    producer.send(message, attributes).get();

    // Assert
    verify(mockSqsAsyncClient).sendMessage(any(SendMessageRequest.class));
  }
}
