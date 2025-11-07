package com.dream11.queue.impl.sns;

import com.dream11.queue.util.MessageAttributeConverter;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sns.SnsAsyncClient;
import software.amazon.awssdk.services.sns.SnsAsyncClientBuilder;
import software.amazon.awssdk.services.sns.model.MessageAttributeValue;
import software.amazon.awssdk.services.sns.model.PublishRequest;

/**
 * Client for interacting with Amazon SNS. This class handles the low-level operations of publishing
 * messages to an SNS topic.
 */
public class SnsClient {
  private final SnsConfig snsConfig;
  private final SnsAsyncClient snsAsyncClient;

  /**
   * Constructs a new SnsClient with the given configuration and SNS client. This constructor is
   * useful when you want to provide a custom SNS client.
   *
   * @param snsConfig The SNS configuration.
   * @param snsAsyncClient The SNS async client.
   */
  public SnsClient(SnsConfig snsConfig, SnsAsyncClient snsAsyncClient) {
    this.snsConfig = snsConfig;
    this.snsAsyncClient = snsAsyncClient;
  }

  /**
   * Constructs a new SnsClient with the given configuration. Creates a new SNS client using the
   * default credentials provider and the specified region. If an endpoint is provided in the
   * configuration, it will be used instead of the default endpoint.
   *
   * @param snsConfig The SNS configuration.
   */
  public SnsClient(SnsConfig snsConfig) {
    this.snsConfig = snsConfig;
    SnsAsyncClientBuilder snsClientBuilder =
        SnsAsyncClient.builder()
            .credentialsProvider(DefaultCredentialsProvider.create())
            .region(Region.of(snsConfig.getRegion()));
    if (this.snsConfig.getEndpoint() != null && !this.snsConfig.getEndpoint().isEmpty()) {
      snsClientBuilder.endpointOverride(URI.create(snsConfig.getEndpoint()));
    }
    this.snsAsyncClient = snsClientBuilder.build();
  }

  /**
   * Publishes a message asynchronously to the SNS topic.
   *
   * @param message The message to publish.
   * @return A CompletableFuture that completes when the message is published.
   */
  public CompletableFuture<Void> publish(String message) {
    return publish(message, null);
  }

  /**
   * Publishes a message asynchronously to the SNS topic with custom attributes.
   *
   * @param message The message to publish.
   * @param attributes User-defined message attributes (can be null or empty).
   * @return A CompletableFuture that completes when the message is published.
   */
  public CompletableFuture<Void> publish(String message, Map<String, Object> attributes) {
    PublishRequest.Builder requestBuilder =
        PublishRequest.builder().topicArn(snsConfig.getTopicArn()).message(message);

    if (attributes != null && !attributes.isEmpty()) {
      Map<String, MessageAttributeValue> messageAttributes =
          MessageAttributeConverter.convert(
              attributes,
              (dataType, stringValue) ->
                  MessageAttributeValue.builder()
                      .dataType(dataType)
                      .stringValue(stringValue)
                      .build());
      requestBuilder.messageAttributes(messageAttributes);
    }

    return this.snsAsyncClient.publish(requestBuilder.build()).thenAccept(__ -> {});
  }

  /** Closes the SNS client, releasing any resources. */
  public void close() {
    this.snsAsyncClient.close();
  }
}
