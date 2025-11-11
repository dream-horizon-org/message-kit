package com.dream11.queue.producer;

import com.dream11.queue.config.QueueConfig;
import com.dream11.queue.impl.sns.SnsConfig;
import com.dream11.queue.impl.sns.SnsProducer;
import com.dream11.queue.impl.sqs.SqsConfig;
import com.dream11.queue.impl.sqs.SqsProducer;
import lombok.NonNull;
import lombok.experimental.UtilityClass;

/**
 * Factory class for creating MessageProducer instances. This factory supports different message
 * queue providers and creates the appropriate producer implementation based on the provided
 * configuration.
 */
@UtilityClass
public class MessageProducerFactory {

  /**
   * Creates a MessageProducer based on the provided configuration. The type of producer created
   * depends on the provider specified in the configuration.
   *
   * @param config The configuration for the message producer.
   * @return A new MessageProducer instance.
   * @throws IllegalArgumentException if the provider type is invalid.
   * @param <T> The type of message that the producer will handle.
   */
  public <T> MessageProducer<T> create(@NonNull QueueConfig config) {
    switch (config.getProvider()) {
      case SQS:
        return new SqsProducer<>((SqsConfig) config);
      case SNS:
        return new SnsProducer<>((SnsConfig) config);
      default:
        throw new IllegalArgumentException(
            "Invalid message producer type: " + config.getProvider());
    }
  }
}
