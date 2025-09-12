package com.dream11.queue.consumer;

import com.dream11.queue.Message;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Interface for consuming messages from a message queue. Implementations of this interface handle
 * the specifics of receiving and acknowledging messages from different message queue providers.
 */
public interface MessageConsumer extends AutoCloseable {
  /**
   * Receives a list of messages asynchronously. The number of messages received will depend on the
   * configuration of the consumer.
   *
   * @return A CompletableFuture containing a list of received messages.
   */
  CompletableFuture<List<Message>> receive();

  /**
   * Receives a list of messages asynchronously with a specified timeout. The number of messages
   * received will depend on the configuration of the consumer.
   *
   * @param timeout The timeout in seconds to wait for messages.
   * @return A CompletableFuture containing a list of received messages.
   */
  CompletableFuture<List<Message>> receive(int timeout);

  /**
   * Acknowledges a message, indicating that it has been processed. This typically removes the
   * message from the queue or marks it as processed.
   *
   * @param message The message to acknowledge.
   * @return A CompletableFuture that completes when the message is acknowledged.
   */
  CompletableFuture<Void> acknowledgeMessage(Message message);

  /**
   * Sends a heartbeat for the given message.
   *
   * @param message The message for which the heartbeat is sent.
   * @return A CompletableFuture that completes when the heartbeat is sent.
   */
  CompletableFuture<Void> sendHeartbeat(Message message);

  /**
   * Closes the message consumer, releasing any resources. This method should be called when the
   * consumer is no longer needed.
   */
  void close();
}
