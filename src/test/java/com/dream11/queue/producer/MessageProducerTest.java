package com.dream11.queue.producer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

class MessageProducerTest {

  @Test
  void testSendWithAttributesThrowsUnsupportedOperationExceptionByDefault() {
    // Arrange
    MessageProducer<String> producer = new TestMessageProducerWithoutAttributeSupport();
    Map<String, Object> attributes = new HashMap<>();
    attributes.put("key1", "value1");

    // Act & Assert
    assertThatThrownBy(() -> producer.send("test message", attributes))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Message attributes are not supported by this provider");
  }

  @Test
  void testSupportsMessageAttributesReturnsFalseByDefault() {
    // Arrange
    MessageProducer<String> producer = new TestMessageProducerWithoutAttributeSupport();

    // Act
    boolean supportsAttributes = producer.supportsMessageAttributes();

    // Assert
    assertThat(supportsAttributes).isFalse();
  }

  /** Test implementation that uses default methods (no attribute support). */
  private static class TestMessageProducerWithoutAttributeSupport
      implements MessageProducer<String> {
    @Override
    public CompletableFuture<Void> send(String message) {
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public void close() {
      // No-op for testing
    }
  }
}
