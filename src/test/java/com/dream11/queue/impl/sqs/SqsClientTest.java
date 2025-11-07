package com.dream11.queue.impl.sqs;

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
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;

class SqsClientTest {

  private SqsAsyncClient mockSqsAsyncClient;
  private SqsClient sqsClient;

  @BeforeEach
  void setUp() {
    SqsConfig sqsConfig =
        SqsConfig.builder().region("us-east-1").queueUrl("test-queue-url").build();
    mockSqsAsyncClient = mock(SqsAsyncClient.class);
    when(mockSqsAsyncClient.sendMessage(any(SendMessageRequest.class)))
        .thenReturn(CompletableFuture.completedFuture(SendMessageResponse.builder().build()));
    sqsClient = new SqsClient(sqsConfig, mockSqsAsyncClient);
  }

  @Test
  void testSendWithoutAttributes() throws Exception {
    // Arrange
    String message = "test message";

    // Act
    sqsClient.send(message).get();

    // Assert
    ArgumentCaptor<SendMessageRequest> captor = ArgumentCaptor.forClass(SendMessageRequest.class);
    verify(mockSqsAsyncClient).sendMessage(captor.capture());
    SendMessageRequest request = captor.getValue();
    assertThat(request.messageBody()).isEqualTo(message);
    assertThat(request.queueUrl()).isEqualTo("test-queue-url");
    assertThat(request.messageAttributes()).isNullOrEmpty();
  }

  @Test
  void testSendWithStringAttributes() throws Exception {
    // Arrange
    String message = "test message";
    Map<String, Object> attributes = new HashMap<>();
    attributes.put("key1", "value1");
    attributes.put("key2", "value2");

    // Act
    sqsClient.send(message, attributes).get();

    // Assert
    ArgumentCaptor<SendMessageRequest> captor = ArgumentCaptor.forClass(SendMessageRequest.class);
    verify(mockSqsAsyncClient).sendMessage(captor.capture());
    SendMessageRequest request = captor.getValue();
    assertThat(request.messageBody()).isEqualTo(message);
    assertThat(request.queueUrl()).isEqualTo("test-queue-url");
    assertThat(request.messageAttributes()).hasSize(2);
    assertThat(request.messageAttributes().get("key1").dataType()).isEqualTo("String");
    assertThat(request.messageAttributes().get("key1").stringValue()).isEqualTo("value1");
    assertThat(request.messageAttributes().get("key2").dataType()).isEqualTo("String");
    assertThat(request.messageAttributes().get("key2").stringValue()).isEqualTo("value2");
  }

  @Test
  void testSendWithNumberAttributes() throws Exception {
    // Arrange
    String message = "test message";
    Map<String, Object> attributes = new HashMap<>();
    attributes.put("priority", 10);
    attributes.put("score", 99.5);

    // Act
    sqsClient.send(message, attributes).get();

    // Assert
    ArgumentCaptor<SendMessageRequest> captor = ArgumentCaptor.forClass(SendMessageRequest.class);
    verify(mockSqsAsyncClient).sendMessage(captor.capture());
    SendMessageRequest request = captor.getValue();
    assertThat(request.messageBody()).isEqualTo(message);
    assertThat(request.messageAttributes()).hasSize(2);
    assertThat(request.messageAttributes().get("priority").dataType()).isEqualTo("Number");
    assertThat(request.messageAttributes().get("priority").stringValue()).isEqualTo("10");
    assertThat(request.messageAttributes().get("score").dataType()).isEqualTo("Number");
    assertThat(request.messageAttributes().get("score").stringValue()).isEqualTo("99.5");
  }

  @Test
  void testSendWithMixedTypeAttributes() throws Exception {
    // Arrange
    String message = "test message";
    Map<String, Object> attributes = new HashMap<>();
    attributes.put("stringAttr", "value");
    attributes.put("numberAttr", 42);
    attributes.put("booleanAttr", true);

    // Act
    sqsClient.send(message, attributes).get();

    // Assert
    ArgumentCaptor<SendMessageRequest> captor = ArgumentCaptor.forClass(SendMessageRequest.class);
    verify(mockSqsAsyncClient).sendMessage(captor.capture());
    SendMessageRequest request = captor.getValue();
    assertThat(request.messageAttributes()).hasSize(3);
    assertThat(request.messageAttributes().get("stringAttr").dataType()).isEqualTo("String");
    assertThat(request.messageAttributes().get("stringAttr").stringValue()).isEqualTo("value");
    assertThat(request.messageAttributes().get("numberAttr").dataType()).isEqualTo("Number");
    assertThat(request.messageAttributes().get("numberAttr").stringValue()).isEqualTo("42");
    assertThat(request.messageAttributes().get("booleanAttr").dataType()).isEqualTo("String");
    assertThat(request.messageAttributes().get("booleanAttr").stringValue()).isEqualTo("true");
  }

  @Test
  void testSendWithEmptyAttributes() throws Exception {
    // Arrange
    String message = "test message";
    Map<String, Object> attributes = new HashMap<>();

    // Act
    sqsClient.send(message, attributes).get();

    // Assert
    ArgumentCaptor<SendMessageRequest> captor = ArgumentCaptor.forClass(SendMessageRequest.class);
    verify(mockSqsAsyncClient).sendMessage(captor.capture());
    SendMessageRequest request = captor.getValue();
    assertThat(request.messageBody()).isEqualTo(message);
    assertThat(request.messageAttributes()).isNullOrEmpty();
  }

  @Test
  void testSendWithNullAttributes() throws Exception {
    // Arrange
    String message = "test message";

    // Act
    sqsClient.send(message, null).get();

    // Assert
    ArgumentCaptor<SendMessageRequest> captor = ArgumentCaptor.forClass(SendMessageRequest.class);
    verify(mockSqsAsyncClient).sendMessage(captor.capture());
    SendMessageRequest request = captor.getValue();
    assertThat(request.messageBody()).isEqualTo(message);
    assertThat(request.messageAttributes()).isNullOrEmpty();
  }
}
