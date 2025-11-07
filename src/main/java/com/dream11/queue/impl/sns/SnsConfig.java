package com.dream11.queue.impl.sns;

import com.dream11.queue.QueueProvider;
import com.dream11.queue.config.HeartbeatConfig;
import com.dream11.queue.config.QueueConfig;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;

@Getter
@AllArgsConstructor
@Builder
@NoArgsConstructor
public class SnsConfig implements QueueConfig {
  /** The ARN of the SNS topic. */
  @NonNull private String topicArn;
  /** The AWS region where the SNS topic is located. */
  @NonNull private String region;
  /** The endpoint for the SNS topic. */
  private String endpoint;

  /** The configuration for heartbeat settings (not used for SNS, but required by interface). */
  @Builder.Default private HeartbeatConfig heartbeatConfig = new HeartbeatConfig();

  /**
   * Returns the provider type for this configuration.
   *
   * @return The QueueProvider type.
   */
  @Override
  public QueueProvider getProvider() {
    return QueueProvider.SNS;
  }
}
