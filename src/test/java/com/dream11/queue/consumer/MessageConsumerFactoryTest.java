package com.dream11.queue.consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.dream11.queue.impl.sns.SnsConfig;
import com.dream11.queue.impl.sqs.SqsConfig;
import com.dream11.queue.impl.sqs.SqsConsumer;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class MessageConsumerFactoryTest {

  @ParameterizedTest
  @MethodSource("endpoints")
  void testConsumerCreateWhenConfigTypeIsSqsWithEndpointOverride(String endpoint) {
    // Arrange
    SqsConfig sqsConfig =
        SqsConfig.builder().region("us-east-1").queueUrl("queue").endpoint(endpoint).build();

    // Act
    MessageConsumer messageConsumer = MessageConsumerFactory.create(sqsConfig);

    // Assert
    assertThat(messageConsumer).isInstanceOf(SqsConsumer.class);
  }

  @Test
  void testConsumerCreateWhenConfigTypeIsSnsThrowsException() {
    // Arrange
    SnsConfig snsConfig =
        SnsConfig.builder()
            .region("us-east-1")
            .topicArn("arn:aws:sns:us-east-1:123456789012:test-topic")
            .build();

    // Act & Assert
    assertThatThrownBy(() -> MessageConsumerFactory.create(snsConfig))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("SNS does not support consuming messages. Use subscriptions instead.");
  }

  private static Stream<Arguments> endpoints() {
    return Stream.of(Arguments.of("http://dummyEndpoint"), Arguments.of(""));
  }
}
