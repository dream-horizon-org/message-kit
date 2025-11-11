package com.dream11.queue.impl.sns;

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
import software.amazon.awssdk.services.sns.SnsAsyncClient;
import software.amazon.awssdk.services.sns.model.PublishRequest;
import software.amazon.awssdk.services.sns.model.PublishResponse;

class SnsProducerTest {

  private SnsConfig snsConfig;
  private SnsAsyncClient mockSnsAsyncClient;

  @BeforeEach
  void setUp() {
    snsConfig =
        SnsConfig.builder()
            .region("us-east-1")
            .topicArn("arn:aws:sns:us-east-1:123456789012:test-topic")
            .build();
    mockSnsAsyncClient = mock(SnsAsyncClient.class);
    when(mockSnsAsyncClient.publish(any(PublishRequest.class)))
        .thenReturn(CompletableFuture.completedFuture(PublishResponse.builder().build()));
  }

  @Test
  void testSupportsMessageAttributesReturnsTrue() {
    // Arrange
    SnsProducer<String> producer = new SnsProducer<>(snsConfig, mockSnsAsyncClient);

    // Act
    boolean supportsAttributes = producer.supportsMessageAttributes();

    // Assert
    assertThat(supportsAttributes).isTrue();
  }

  @Test
  void testSendWithoutAttributes() throws Exception {
    // Arrange
    SnsProducer<String> producer = new SnsProducer<>(snsConfig, mockSnsAsyncClient);
    String message = "test message";

    // Act
    producer.send(message).get();

    // Assert
    verify(mockSnsAsyncClient).publish(any(PublishRequest.class));
  }

  @Test
  void testSendWithAttributes() throws Exception {
    // Arrange
    SnsProducer<String> producer = new SnsProducer<>(snsConfig, mockSnsAsyncClient);
    String message = "test message";
    Map<String, Object> attributes = new HashMap<>();
    attributes.put("key1", "value1");
    attributes.put("key2", 123);

    // Act
    producer.send(message, attributes).get();

    // Assert
    verify(mockSnsAsyncClient).publish(any(PublishRequest.class));
  }

  @Test
  void testSendWithEmptyAttributes() throws Exception {
    // Arrange
    SnsProducer<String> producer = new SnsProducer<>(snsConfig, mockSnsAsyncClient);
    String message = "test message";
    Map<String, Object> attributes = new HashMap<>();

    // Act
    producer.send(message, attributes).get();

    // Assert
    verify(mockSnsAsyncClient).publish(any(PublishRequest.class));
  }

  @Test
  void testSendWithNullAttributes() throws Exception {
    // Arrange
    SnsProducer<String> producer = new SnsProducer<>(snsConfig, mockSnsAsyncClient);
    String message = "test message";

    // Act
    producer.send(message, Map.of()).get();

    // Assert
    verify(mockSnsAsyncClient).publish(any(PublishRequest.class));
  }

  @Test
  void testSendWithTransformer() throws Exception {
    // Arrange
    SnsProducer<String> producer =
        new SnsProducer<>(snsConfig, mockSnsAsyncClient, String::toUpperCase);
    String message = "test message";

    // Act
    producer.send(message).get();

    // Assert
    verify(mockSnsAsyncClient).publish(any(PublishRequest.class));
  }

  @Test
  void testSendWithTransformerAndAttributes() throws Exception {
    // Arrange
    SnsProducer<String> producer =
        new SnsProducer<>(snsConfig, mockSnsAsyncClient, String::toUpperCase);
    String message = "test message";
    Map<String, Object> attributes = new HashMap<>();
    attributes.put("key1", "value1");

    // Act
    producer.send(message, attributes).get();

    // Assert
    verify(mockSnsAsyncClient).publish(any(PublishRequest.class));
  }
}
