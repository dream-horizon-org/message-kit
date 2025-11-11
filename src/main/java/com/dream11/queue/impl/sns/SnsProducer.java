package com.dream11.queue.impl.sns;

import com.dream11.queue.producer.MessageProducer;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.sns.SnsAsyncClient;

/**
 * Implementation of MessageProducer for Amazon SNS. This producer handles publishing messages to an
 * SNS topic and provides functionality to transform messages before publishing.
 *
 * @param <T> The type of message that this producer will handle.
 */
@Slf4j
public class SnsProducer<T> implements MessageProducer<T> {
  private final SnsClient snsClient;
  private final Function<T, String> transformer;

  /**
   * Constructs a new SnsProducer with the given configuration and default transformer. The default
   * transformer uses Object.toString() to convert messages to strings.
   *
   * @param snsConfig The SNS configuration.
   */
  public SnsProducer(SnsConfig snsConfig) {
    this(snsConfig, Object::toString);
  }

  /**
   * Constructs a new SnsProducer with the given configuration and transformer.
   *
   * @param snsConfig The SNS configuration.
   * @param transformer The function to transform messages from type T to String.
   */
  public SnsProducer(SnsConfig snsConfig, Function<T, String> transformer) {
    this.snsClient = new SnsClient(snsConfig);
    this.transformer = transformer;
  }

  /**
   * Constructs a new SnsProducer with the given configuration and SNS client. Uses the default
   * transformer (Object.toString()).
   *
   * @param snsConfig The SNS configuration.
   * @param snsAsyncClient The SNS async client.
   */
  public SnsProducer(SnsConfig snsConfig, SnsAsyncClient snsAsyncClient) {
    this(snsConfig, snsAsyncClient, Object::toString);
  }

  /**
   * Constructs a new SnsProducer with the given configuration, SNS client, and transformer.
   *
   * @param snsConfig The SNS configuration.
   * @param snsAsyncClient The SNS async client.
   * @param transformer The function to transform messages from type T to String.
   */
  public SnsProducer(
      SnsConfig snsConfig, SnsAsyncClient snsAsyncClient, Function<T, String> transformer) {
    this.snsClient = new SnsClient(snsConfig, snsAsyncClient);
    this.transformer = transformer;
  }

  /**
   * Publishes a message asynchronously to the SNS topic. The message is transformed to a string
   * before publishing.
   *
   * @param message The message to publish.
   * @return A CompletableFuture that completes when the message is published.
   */
  @Override
  public CompletableFuture<Void> send(T message) {
    return this.snsClient.publish(transformer.apply(message));
  }

  /**
   * Publishes a message asynchronously to the SNS topic with custom attributes. The message is
   * transformed to a string before publishing.
   *
   * @param message The message to publish.
   * @param attributes User-defined message attributes (e.g., routing keys, metadata).
   * @return A CompletableFuture that completes when the message is published.
   */
  @Override
  public CompletableFuture<Void> send(T message, Map<String, Object> attributes) {
    return this.snsClient.publish(transformer.apply(message), attributes);
  }

  /**
   * Returns true as SNS supports message attributes.
   *
   * @return true indicating that message attributes are supported.
   */
  @Override
  public boolean supportsMessageAttributes() {
    return true;
  }

  /**
   * Closes the SNS producer, releasing any resources. This method should be called when the
   * producer is no longer needed.
   */
  @Override
  public void close() {
    this.snsClient.close();
  }
}
