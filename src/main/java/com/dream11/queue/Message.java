package com.dream11.queue;

import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

/**
 * Represents a message in the queue system. A message consists of a body (the actual payload),
 * user-controlled attributes (e.g., routing keys), and system metadata (e.g., message ID, system
 * attributes).
 */
@Getter
@AllArgsConstructor
@Builder
public class Message {

  /** The message body/payload. */
  String body;

  /** System-level metadata populated by the queue provider. */
  Metadata metadata;

  /** User-controlled message attributes (e.g., routingKey for message routing). */
  Map<String, Object> attributes;
}
