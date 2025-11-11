package com.dream11.queue.impl.sqs;

import com.dream11.queue.producer.MessageProducer;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;

/**
 * Implementation of MessageProducer for Amazon SQS. This producer handles sending messages to an
 * SQS queue and provides functionality to transform messages before sending.
 *
 * @param <T> The type of message that this producer will handle.
 */
@Slf4j
public class SqsProducer<T> implements MessageProducer<T> {
  private final SqsClient sqsClient;
  private final Function<T, String> transformer;

  /**
   * Constructs a new SqsProducer with the given configuration and default transformer. The default
   * transformer uses Object.toString() to convert messages to strings.
   *
   * @param sqsConfig The SQS configuration.
   */
  public SqsProducer(SqsConfig sqsConfig) {
    this(sqsConfig, Object::toString);
  }

  /**
   * Constructs a new SqsProducer with the given configuration and transformer.
   *
   * @param sqsConfig The SQS configuration.
   * @param transformer The function to transform messages from type T to String.
   */
  public SqsProducer(SqsConfig sqsConfig, Function<T, String> transformer) {
    this.sqsClient = new SqsClient(sqsConfig);
    this.transformer = transformer;
  }

  /**
   * Constructs a new SqsProducer with the given configuration and SQS client. Uses the default
   * transformer (Object.toString()).
   *
   * @param sqsConfig The SQS configuration.
   * @param sqsAsyncClient The SQS async client.
   */
  public SqsProducer(SqsConfig sqsConfig, SqsAsyncClient sqsAsyncClient) {
    this(sqsConfig, sqsAsyncClient, Object::toString);
  }

  /**
   * Constructs a new SqsProducer with the given configuration, SQS client, and transformer.
   *
   * @param sqsConfig The SQS configuration.
   * @param sqsAsyncClient The SQS async client.
   * @param transformer The function to transform messages from type T to String.
   */
  public SqsProducer(
      SqsConfig sqsConfig, SqsAsyncClient sqsAsyncClient, Function<T, String> transformer) {
    this.sqsClient = new SqsClient(sqsConfig, sqsAsyncClient);
    this.transformer = transformer;
  }

  /**
   * Sends a message asynchronously to the SQS queue. The message is transformed to a string before
   * sending.
   *
   * @param message The message to send.
   * @return A CompletableFuture that completes when the message is sent.
   */
  @Override
  public CompletableFuture<Void> send(T message) {
    return this.sqsClient.send(transformer.apply(message));
  }

  /**
   * Sends a message asynchronously to the SQS queue with custom attributes. The message is
   * transformed to a string before sending.
   *
   * @param message The message to send.
   * @param attributes User-defined message attributes (e.g., routing keys, metadata).
   * @return A CompletableFuture that completes when the message is sent.
   */
  @Override
  public CompletableFuture<Void> send(T message, Map<String, Object> attributes) {
    return this.sqsClient.send(transformer.apply(message), attributes);
  }

  /**
   * Returns true as SQS supports message attributes.
   *
   * @return true indicating that message attributes are supported.
   */
  @Override
  public boolean supportsMessageAttributes() {
    return true;
  }

  /**
   * Closes the SQS producer, releasing any resources. This method should be called when the
   * producer is no longer needed.
   */
  @Override
  public void close() {
    this.sqsClient.close();
  }
}
