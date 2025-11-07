package com.dream11.queue.consumer;

import com.dream11.queue.config.QueueConfig;
import com.dream11.queue.impl.sqs.SqsConfig;
import com.dream11.queue.impl.sqs.SqsConsumer;
import lombok.NonNull;
import lombok.experimental.UtilityClass;

/**
 * Factory class for creating MessageConsumer instances. This factory supports different message
 * queue providers and creates the appropriate consumer implementation based on the provided
 * configuration.
 */
@UtilityClass
public class MessageConsumerFactory {

  /**
   * Creates a MessageConsumer based on the provided configuration. The type of consumer created
   * depends on the provider specified in the configuration.
   *
   * @param config The configuration for the message consumer.
   * @return A new MessageConsumer instance.
   * @throws IllegalArgumentException if the provider type is invalid.
   */
  @SuppressWarnings("unchecked")
  public MessageConsumer create(@NonNull QueueConfig config) {
    switch (config.getProvider()) {
      case SQS:
        return new SqsConsumer((SqsConfig) config);
      case SNS:
        throw new UnsupportedOperationException(
            "SNS does not support consuming messages. Use subscriptions instead.");
      default:
        throw new IllegalArgumentException(
            "Invalid message consumer type: " + config.getProvider());
    }
  }
}
