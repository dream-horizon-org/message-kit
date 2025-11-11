package com.dream11.queue.util;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;
import lombok.experimental.UtilityClass;

/**
 * Utility class for converting message attributes to AWS-specific MessageAttributeValue objects.
 * This class provides a generic way to convert Map&lt;String, Object&gt; to service-specific
 * message attribute formats used by AWS services like SQS and SNS.
 *
 * <p>Supported types:
 *
 * <ul>
 *   <li>String: Converted as-is to AWS String type
 *   <li>Number (Integer, Long, Float, Double, etc.): Converted to AWS Number type
 *   <li>Boolean: Converted to AWS String type ("true" or "false") - AWS has no native Boolean type
 * </ul>
 *
 * <p>Unsupported types (will throw IllegalArgumentException):
 *
 * <ul>
 *   <li>Binary data (byte[])
 *   <li>Collections (List, Set, Array)
 *   <li>Maps
 *   <li>Custom objects
 *   <li>null values
 * </ul>
 */
@UtilityClass
public class MessageAttributeConverter {

  /**
   * Converts a map of generic objects to AWS MessageAttributeValue objects. This method supports
   * String, Number, and Boolean types only. Unsupported types will cause an
   * IllegalArgumentException.
   *
   * @param attributes The attributes to convert (can be null or empty, must not contain null
   *     values)
   * @param attributeValueFactory Factory function that creates a service-specific
   *     MessageAttributeValue given a dataType and stringValue
   * @param <T> The AWS MessageAttributeValue type (e.g., SQS or SNS MessageAttributeValue)
   * @return A map of converted MessageAttributeValue objects
   * @throws IllegalArgumentException if any attribute value is null or of an unsupported type
   */
  public <T> Map<String, T> convert(
      Map<String, Object> attributes, BiFunction<String, String, T> attributeValueFactory) {

    if (attributes == null || attributes.isEmpty()) {
      return Map.of();
    }

    Map<String, T> messageAttributes = new HashMap<>();
    attributes.forEach(
        (key, value) -> {
          if (value == null) {
            throw new IllegalArgumentException("Message attribute '" + key + "' cannot be null");
          }

          String dataType;
          String stringValue;

          if (value instanceof String || value instanceof Boolean) {
            dataType = "String";
            stringValue = value.toString();
          } else if (value instanceof Number) {
            dataType = "Number";
            stringValue = value.toString();
          } else {
            throw new IllegalArgumentException(
                "Unsupported message attribute type for key '"
                    + key
                    + "': "
                    + value.getClass().getName()
                    + ". Supported types: String, Number, Boolean");
          }

          messageAttributes.put(key, attributeValueFactory.apply(dataType, stringValue));
        });
    return messageAttributes;
  }
}
