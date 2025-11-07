package com.dream11.queue;

import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

/**
 * System-level metadata associated with a message from the queue provider. This includes
 * information like message ID and system attributes that are populated by the queue system.
 */
@Getter
@AllArgsConstructor
@Builder
public class Metadata {
  /** The message ID assigned by the queue provider. */
  String id;

  /** System-level attributes populated by the queue provider (e.g., timestamps, retry count). */
  Map<String, Object> attributes;
}
