package com.dream11.queue.impl.sqs;

import com.dream11.queue.Message;
import com.dream11.queue.consumer.MessageConsumer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;

/**
 * Implementation of MessageConsumer for Amazon SQS. This consumer handles receiving and
 * acknowledging messages from an SQS queue.
 */
@Slf4j
public class SqsConsumer implements MessageConsumer {

  private static final String RECEIPT_HANDLE = "RECEIPT_HANDLE";
  private final SqsClient sqsClient;

  @Getter private final SqsConfig sqsConfig;

  /**
   * A map to store scheduled futures for heartbeats. The key is the message ID, and the value is
   * the ScheduledFuture.
   */
  private final Map<String, ScheduledFuture<?>> heartbeatFutures = new ConcurrentHashMap<>();

  private final ScheduledExecutorService executorService;

  /**
   * Constructs a new SqsConsumer with the given configuration. Creates a new SQS client using the
   * provided configuration.
   *
   * @param sqsConfig The SQS configuration.
   */
  public SqsConsumer(SqsConfig sqsConfig) {
    this.sqsClient = new SqsClient(sqsConfig);
    this.sqsConfig = sqsConfig;
    this.executorService =
        Executors.newScheduledThreadPool(
            sqsConfig.getHeartbeatConfig().getExecutorThreadPoolSize());
  }

  /**
   * Constructs a new SqsConsumer with the given configuration and SQS client. This constructor is
   * useful when you want to provide a custom SQS client.
   *
   * @param sqsConfig The SQS configuration.
   * @param sqsAsyncClient The SQS async client.
   */
  public SqsConsumer(SqsConfig sqsConfig, SqsAsyncClient sqsAsyncClient) {
    this.sqsClient = new SqsClient(sqsConfig, sqsAsyncClient);
    this.sqsConfig = sqsConfig;
    this.executorService =
        Executors.newScheduledThreadPool(
            sqsConfig.getHeartbeatConfig().getExecutorThreadPoolSize());
  }

  /**
   * Receives a list of messages asynchronously from the SQS queue. The number of messages received
   * is determined by the configuration.
   *
   * @return A CompletableFuture containing a list of received messages.
   */
  @Override
  public CompletableFuture<List<Message>> receive() {
    return this.receive(0);
  }

  /**
   * Receives a list of messages asynchronously with a specified timeout. The number of messages
   * received is determined by the configuration.
   *
   * @param timeout The timeout in seconds to wait for messages.
   * @return A CompletableFuture containing a list of received messages.
   */
  @Override
  public CompletableFuture<List<Message>> receive(int timeout) {
    return this.sqsClient
        .receive(timeout)
        .thenApply(
            messages -> messages.stream().map(this::buildMessage).collect(Collectors.toList()))
        .thenApply(
            messages -> {
              if (this.getSqsConfig().getHeartbeatConfig().getHeartbeatInterval() > 0) {
                this.sendHeartbeats(messages);
              }
              return messages;
            });
  }

  /**
   * Acknowledges a message by deleting it from the SQS queue. This indicates that the message has
   * been successfully processed.
   *
   * @param message The message to acknowledge.
   * @return A CompletableFuture that completes when the message is deleted.
   */
  @Override
  public CompletableFuture<Void> acknowledgeMessage(Message message) {
    return this.sqsClient
        .deleteMessage(this.getReceiptHandle(message))
        .thenAccept(
            v -> {
              ScheduledFuture<?> future = this.heartbeatFutures.remove(message.getId());
              if (future != null) {
                future.cancel(true);
              }
            });
  }

  /**
   * Sends a heartbeat for the given message.
   *
   * @param message The message for which the heartbeat is sent.
   * @return A CompletableFuture that completes when the heartbeat is sent.
   */
  @Override
  public CompletableFuture<Void> sendHeartbeat(Message message) {
    return this.sqsClient.changeMessageVisibility(
        this.getReceiptHandle(message),
        this.getSqsConfig().getHeartbeatConfig().getHeartbeatInterval() * 2);
  }

  /**
   * Closes the SQS consumer, releasing any resources. This method should be called when the
   * consumer is no longer needed.
   */
  @Override
  public void close() {
    this.heartbeatFutures.values().forEach(future -> future.cancel(true));
    this.executorService.shutdown();
    this.sqsClient.close();
  }

  private void sendHeartbeats(List<Message> messages) {
    messages.forEach(
        message -> {
          if (this.heartbeatFutures.containsKey(message.getId())) {
            this.heartbeatFutures.get(message.getId()).cancel(true);
          }
          this.heartbeatFutures.put(
              message.getId(),
              this.executorService.scheduleAtFixedRate(
                  () -> {
                    try {
                      this.sendHeartbeat(message);
                    } catch (Exception e) {
                      log.error("Failed to send heartbeat for message: {}", message, e);
                    }
                  },
                  this.getSqsConfig().getHeartbeatConfig().getHeartbeatInterval(),
                  this.getSqsConfig().getHeartbeatConfig().getHeartbeatInterval(),
                  TimeUnit.SECONDS));
        });
  }

  private Message buildMessage(software.amazon.awssdk.services.sqs.model.Message message) {
    return Message.builder()
        .body(message.body())
        .id(message.messageId())
        .attributes(Map.of(RECEIPT_HANDLE, message.receiptHandle()))
        .build();
  }

  private String getReceiptHandle(Message message) {
    return message.getAttributes().get(RECEIPT_HANDLE).toString();
  }
}
