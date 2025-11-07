package com.dream11.queue.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class MessageAttributeConverterTest {

  @Test
  void testConvertStringAttribute() {
    // Arrange
    Map<String, Object> attributes = Map.of("key", "value");

    // Act
    Map<String, TestAttributeValue> result =
        MessageAttributeConverter.convert(attributes, TestAttributeValue::new);

    // Assert
    assertThat(result).hasSize(1);
    assertThat(result.get("key").dataType).isEqualTo("String");
    assertThat(result.get("key").stringValue).isEqualTo("value");
  }

  @Test
  void testConvertNumberAttribute() {
    // Arrange
    Map<String, Object> attributes = new HashMap<>();
    attributes.put("integer", 42);
    attributes.put("long", 123L);
    attributes.put("double", 99.5);

    // Act
    Map<String, TestAttributeValue> result =
        MessageAttributeConverter.convert(attributes, TestAttributeValue::new);

    // Assert
    assertThat(result).hasSize(3);
    assertThat(result.get("integer").dataType).isEqualTo("Number");
    assertThat(result.get("integer").stringValue).isEqualTo("42");
    assertThat(result.get("long").dataType).isEqualTo("Number");
    assertThat(result.get("long").stringValue).isEqualTo("123");
    assertThat(result.get("double").dataType).isEqualTo("Number");
    assertThat(result.get("double").stringValue).isEqualTo("99.5");
  }

  @Test
  void testConvertBooleanAttribute() {
    // Arrange
    Map<String, Object> attributes = new HashMap<>();
    attributes.put("trueValue", true);
    attributes.put("falseValue", false);

    // Act
    Map<String, TestAttributeValue> result =
        MessageAttributeConverter.convert(attributes, TestAttributeValue::new);

    // Assert
    assertThat(result).hasSize(2);
    assertThat(result.get("trueValue").dataType).isEqualTo("String");
    assertThat(result.get("trueValue").stringValue).isEqualTo("true");
    assertThat(result.get("falseValue").dataType).isEqualTo("String");
    assertThat(result.get("falseValue").stringValue).isEqualTo("false");
  }

  @Test
  void testConvertMixedTypes() {
    // Arrange
    Map<String, Object> attributes = new HashMap<>();
    attributes.put("string", "value");
    attributes.put("number", 42);
    attributes.put("boolean", true);

    // Act
    Map<String, TestAttributeValue> result =
        MessageAttributeConverter.convert(attributes, TestAttributeValue::new);

    // Assert
    assertThat(result).hasSize(3);
    assertThat(result.get("string").dataType).isEqualTo("String");
    assertThat(result.get("number").dataType).isEqualTo("Number");
    assertThat(result.get("boolean").dataType).isEqualTo("String");
  }

  @Test
  void testConvertWithNullValueThrowsException() {
    // Arrange
    Map<String, Object> attributes = new HashMap<>();
    attributes.put("nullValue", null);

    // Act & Assert
    assertThatThrownBy(() -> MessageAttributeConverter.convert(attributes, TestAttributeValue::new))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Message attribute 'nullValue' cannot be null");
  }

  @Test
  void testConvertWithListThrowsException() {
    // Arrange
    Map<String, Object> attributes = Map.of("list", List.of("a", "b", "c"));

    // Act & Assert
    assertThatThrownBy(() -> MessageAttributeConverter.convert(attributes, TestAttributeValue::new))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unsupported message attribute type for key 'list'")
        .hasMessageContaining("Supported types: String, Number, Boolean");
  }

  @Test
  void testConvertWithMapThrowsException() {
    // Arrange
    Map<String, Object> attributes = Map.of("map", Map.of("nested", "value"));

    // Act & Assert
    assertThatThrownBy(() -> MessageAttributeConverter.convert(attributes, TestAttributeValue::new))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unsupported message attribute type for key 'map'")
        .hasMessageContaining("Supported types: String, Number, Boolean");
  }

  @Test
  void testConvertWithByteArrayThrowsException() {
    // Arrange
    Map<String, Object> attributes = Map.of("bytes", new byte[] {1, 2, 3});

    // Act & Assert
    assertThatThrownBy(() -> MessageAttributeConverter.convert(attributes, TestAttributeValue::new))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unsupported message attribute type for key 'bytes'")
        .hasMessageContaining("Supported types: String, Number, Boolean");
  }

  @Test
  void testConvertWithCustomObjectThrowsException() {
    // Arrange
    Map<String, Object> attributes = Map.of("custom", new CustomObject("test"));

    // Act & Assert
    assertThatThrownBy(() -> MessageAttributeConverter.convert(attributes, TestAttributeValue::new))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unsupported message attribute type for key 'custom'")
        .hasMessageContaining("CustomObject")
        .hasMessageContaining("Supported types: String, Number, Boolean");
  }

  @Test
  void testConvertEmptyMap() {
    // Arrange
    Map<String, Object> attributes = Map.of();

    // Act
    Map<String, TestAttributeValue> result =
        MessageAttributeConverter.convert(attributes, TestAttributeValue::new);

    // Assert
    assertThat(result).isEmpty();
  }

  // Test helper classes
  static class TestAttributeValue {
    final String dataType;
    final String stringValue;

    TestAttributeValue(String dataType, String stringValue) {
      this.dataType = dataType;
      this.stringValue = stringValue;
    }
  }

  static class CustomObject {
    final String value;

    CustomObject(String value) {
      this.value = value;
    }
  }
}
