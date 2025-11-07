package com.dream11.queue.impl.sns;

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
import software.amazon.awssdk.services.sns.SnsAsyncClient;
import software.amazon.awssdk.services.sns.model.PublishRequest;
import software.amazon.awssdk.services.sns.model.PublishResponse;

class SnsClientTest {

  private SnsAsyncClient mockSnsAsyncClient;
  private SnsClient snsClient;

  @BeforeEach
  void setUp() {
    SnsConfig snsConfig =
        SnsConfig.builder()
            .region("us-east-1")
            .topicArn("arn:aws:sns:us-east-1:123456789012:test-topic")
            .build();
    mockSnsAsyncClient = mock(SnsAsyncClient.class);
    when(mockSnsAsyncClient.publish(any(PublishRequest.class)))
        .thenReturn(CompletableFuture.completedFuture(PublishResponse.builder().build()));
    snsClient = new SnsClient(snsConfig, mockSnsAsyncClient);
  }

  @Test
  void testPublishWithoutAttributes() throws Exception {
    // Arrange
    String message = "test message";

    // Act
    snsClient.publish(message).get();

    // Assert
    ArgumentCaptor<PublishRequest> captor = ArgumentCaptor.forClass(PublishRequest.class);
    verify(mockSnsAsyncClient).publish(captor.capture());
    PublishRequest request = captor.getValue();
    assertThat(request.message()).isEqualTo(message);
    assertThat(request.topicArn()).isEqualTo("arn:aws:sns:us-east-1:123456789012:test-topic");
    assertThat(request.messageAttributes()).isNullOrEmpty();
  }

  @Test
  void testPublishWithStringAttributes() throws Exception {
    // Arrange
    String message = "test message";
    Map<String, Object> attributes = new HashMap<>();
    attributes.put("key1", "value1");
    attributes.put("key2", "value2");

    // Act
    snsClient.publish(message, attributes).get();

    // Assert
    ArgumentCaptor<PublishRequest> captor = ArgumentCaptor.forClass(PublishRequest.class);
    verify(mockSnsAsyncClient).publish(captor.capture());
    PublishRequest request = captor.getValue();
    assertThat(request.message()).isEqualTo(message);
    assertThat(request.topicArn()).isEqualTo("arn:aws:sns:us-east-1:123456789012:test-topic");
    assertThat(request.messageAttributes()).hasSize(2);
    assertThat(request.messageAttributes().get("key1").dataType()).isEqualTo("String");
    assertThat(request.messageAttributes().get("key1").stringValue()).isEqualTo("value1");
    assertThat(request.messageAttributes().get("key2").dataType()).isEqualTo("String");
    assertThat(request.messageAttributes().get("key2").stringValue()).isEqualTo("value2");
  }

  @Test
  void testPublishWithNumberAttributes() throws Exception {
    // Arrange
    String message = "test message";
    Map<String, Object> attributes = new HashMap<>();
    attributes.put("priority", 10);
    attributes.put("score", 99.5);

    // Act
    snsClient.publish(message, attributes).get();

    // Assert
    ArgumentCaptor<PublishRequest> captor = ArgumentCaptor.forClass(PublishRequest.class);
    verify(mockSnsAsyncClient).publish(captor.capture());
    PublishRequest request = captor.getValue();
    assertThat(request.message()).isEqualTo(message);
    assertThat(request.messageAttributes()).hasSize(2);
    assertThat(request.messageAttributes().get("priority").dataType()).isEqualTo("Number");
    assertThat(request.messageAttributes().get("priority").stringValue()).isEqualTo("10");
    assertThat(request.messageAttributes().get("score").dataType()).isEqualTo("Number");
    assertThat(request.messageAttributes().get("score").stringValue()).isEqualTo("99.5");
  }

  @Test
  void testPublishWithMixedTypeAttributes() throws Exception {
    // Arrange
    String message = "test message";
    Map<String, Object> attributes = new HashMap<>();
    attributes.put("stringAttr", "value");
    attributes.put("numberAttr", 42);
    attributes.put("booleanAttr", true);

    // Act
    snsClient.publish(message, attributes).get();

    // Assert
    ArgumentCaptor<PublishRequest> captor = ArgumentCaptor.forClass(PublishRequest.class);
    verify(mockSnsAsyncClient).publish(captor.capture());
    PublishRequest request = captor.getValue();
    assertThat(request.messageAttributes()).hasSize(3);
    assertThat(request.messageAttributes().get("stringAttr").dataType()).isEqualTo("String");
    assertThat(request.messageAttributes().get("stringAttr").stringValue()).isEqualTo("value");
    assertThat(request.messageAttributes().get("numberAttr").dataType()).isEqualTo("Number");
    assertThat(request.messageAttributes().get("numberAttr").stringValue()).isEqualTo("42");
    assertThat(request.messageAttributes().get("booleanAttr").dataType()).isEqualTo("String");
    assertThat(request.messageAttributes().get("booleanAttr").stringValue()).isEqualTo("true");
  }

  @Test
  void testPublishWithEmptyAttributes() throws Exception {
    // Arrange
    String message = "test message";
    Map<String, Object> attributes = new HashMap<>();

    // Act
    snsClient.publish(message, attributes).get();

    // Assert
    ArgumentCaptor<PublishRequest> captor = ArgumentCaptor.forClass(PublishRequest.class);
    verify(mockSnsAsyncClient).publish(captor.capture());
    PublishRequest request = captor.getValue();
    assertThat(request.message()).isEqualTo(message);
    assertThat(request.messageAttributes()).isNullOrEmpty();
  }

  @Test
  void testPublishWithNullAttributes() throws Exception {
    // Arrange
    String message = "test message";

    // Act
    snsClient.publish(message, null).get();

    // Assert
    ArgumentCaptor<PublishRequest> captor = ArgumentCaptor.forClass(PublishRequest.class);
    verify(mockSnsAsyncClient).publish(captor.capture());
    PublishRequest request = captor.getValue();
    assertThat(request.message()).isEqualTo(message);
    assertThat(request.messageAttributes()).isNullOrEmpty();
  }
}
