package com.dream11.queue;

import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

@Getter
@AllArgsConstructor
@Builder
public class Message {

  String body;
  String id;
  Map<String, Object> attributes;
}
