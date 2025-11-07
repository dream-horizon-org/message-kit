package com.dream11.queue.producer;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Interface for producing messages to a message queue. Implementations of this interface handle the
 * specifics of sending messages to different message queue providers.
 *
 * @param <T> The type of message that this producer will handle.
 */
public interface MessageProducer<T> extends AutoCloseable {
  /**
   * Sends a message asynchronously to the message queue.
   *
   * @param message The message to send.
   * @return A CompletableFuture that completes when the message is sent.
   */
  CompletableFuture<Void> send(T message);

  /**
   * Sends a message asynchronously to the message queue with custom attributes.
   *
   * @param message The message to send.
   * @param attributes User-defined message attributes (e.g., routing keys, metadata).
   * @return A CompletableFuture that completes when the message is sent.
   * @throws UnsupportedOperationException if the provider does not support message attributes.
   */
  default CompletableFuture<Void> send(T message, Map<String, Object> attributes) {
    throw new UnsupportedOperationException(
        "Message attributes are not supported by this provider");
  }

  /**
   * Checks if this producer supports sending messages with custom attributes.
   *
   * @return true if message attributes are supported, false otherwise.
   */
  default boolean supportsMessageAttributes() {
    return false;
  }

  /**
   * Closes the message producer, releasing any resources. This method should be called when the
   * producer is no longer needed.
   */
  void close();
}
