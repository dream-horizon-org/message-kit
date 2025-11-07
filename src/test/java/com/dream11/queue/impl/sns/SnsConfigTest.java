package com.dream11.queue.impl.sns;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.dream11.queue.QueueProvider;
import com.dream11.queue.config.HeartbeatConfig;
import org.junit.jupiter.api.Test;

class SnsConfigTest {
  @Test
  void testSnsConfigProvider() {
    // Arrange
    SnsConfig snsConfig =
        SnsConfig.builder()
            .topicArn("arn:aws:sns:us-east-1:123456789012:test-topic")
            .region("us-east-1")
            .heartbeatConfig(
                HeartbeatConfig.builder().executorThreadPoolSize(5).heartbeatInterval(5).build())
            .build();

    // Act and Assert
    assertThat(snsConfig.getProvider()).isEqualTo(QueueProvider.SNS);
    assertThat(snsConfig.getHeartbeatConfig().getHeartbeatInterval()).isEqualTo(5);
    assertThat(snsConfig.getHeartbeatConfig().getExecutorThreadPoolSize()).isEqualTo(5);
  }

  @Test
  void testSnsConfigProviderNoTopicArn() {
    SnsConfig.SnsConfigBuilder builder = SnsConfig.builder().region("us-east-1");
    // Act and Assert
    assertThatThrownBy(builder::build)
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("topicArn is marked non-null but is null");
  }

  @Test
  void testSnsConfigProviderNoRegion() {
    SnsConfig.SnsConfigBuilder builder =
        SnsConfig.builder().topicArn("arn:aws:sns:us-east-1:123456789012:test-topic");
    // Act and Assert
    assertThatThrownBy(builder::build)
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("region is marked non-null but is null");
  }
}
