package com.dream11.queue;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.dream11.queue.impl.sns.SnsConfig;
import com.dream11.queue.impl.sns.SnsProducer;
import com.dream11.queue.impl.sqs.SqsConfig;
import com.dream11.queue.impl.sqs.SqsConsumer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.SneakyThrows;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sns.SnsAsyncClient;
import software.amazon.awssdk.services.sns.model.CreateTopicRequest;
import software.amazon.awssdk.services.sns.model.SubscribeRequest;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesRequest;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import software.amazon.awssdk.services.sqs.model.SetQueueAttributesRequest;

@ExtendWith({Setup.class})
class SnsTopicIT {

  private static SnsProducer<String> SNS_PRODUCER;
  private static SqsConsumer SQS_CONSUMER_1;
  private static SqsConsumer SQS_CONSUMER_2;

  private static SnsAsyncClient SNS_ASYNC_CLIENT;
  private static SqsAsyncClient SQS_ASYNC_CLIENT;

  private static String TOPIC_ARN;
  private static String QUEUE_URL_1;
  private static String QUEUE_URL_2;
  private static String QUEUE_ARN_1;
  private static String QUEUE_ARN_2;

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @BeforeAll
  static void setup() throws Exception {
    // Initialize AWS clients
    SNS_ASYNC_CLIENT =
        SnsAsyncClient.builder()
            .endpointOverride(URI.create(System.getProperty(Constants.SNS_ENDPOINT)))
            .region(Region.of(System.getProperty(Constants.AWS_REGION)))
            .build();

    SQS_ASYNC_CLIENT =
        SqsAsyncClient.builder()
            .endpointOverride(URI.create(System.getProperty(Constants.SQS_ENDPOINT)))
            .region(Region.of(System.getProperty(Constants.AWS_REGION)))
            .build();

    // Create SNS topic
    TOPIC_ARN =
        SNS_ASYNC_CLIENT
            .createTopic(CreateTopicRequest.builder().name("test-topic").build())
            .get()
            .topicArn();

    // Create SQS queues
    QUEUE_URL_1 =
        SQS_ASYNC_CLIENT
            .createQueue(CreateQueueRequest.builder().queueName("test-queue-1").build())
            .get()
            .queueUrl();

    QUEUE_URL_2 =
        SQS_ASYNC_CLIENT
            .createQueue(CreateQueueRequest.builder().queueName("test-queue-2").build())
            .get()
            .queueUrl();

    // Get queue ARNs
    QUEUE_ARN_1 = getQueueArn(QUEUE_URL_1);
    QUEUE_ARN_2 = getQueueArn(QUEUE_URL_2);

    // Set queue policies to allow SNS to send messages
    setQueuePolicy(QUEUE_URL_1, QUEUE_ARN_1, TOPIC_ARN);
    setQueuePolicy(QUEUE_URL_2, QUEUE_ARN_2, TOPIC_ARN);

    // Subscribe queue 1 to SNS topic (no filter)
    SNS_ASYNC_CLIENT
        .subscribe(
            SubscribeRequest.builder()
                .topicArn(TOPIC_ARN)
                .protocol("sqs")
                .endpoint(QUEUE_ARN_1)
                .build())
        .get();

    // Subscribe queue 2 to SNS topic with filter policy
    Map<String, Object> filterPolicy = new HashMap<>();
    filterPolicy.put("routingKey", List.of("queue2"));

    SNS_ASYNC_CLIENT
        .subscribe(
            SubscribeRequest.builder()
                .topicArn(TOPIC_ARN)
                .protocol("sqs")
                .endpoint(QUEUE_ARN_2)
                .attributes(Map.of("FilterPolicy", OBJECT_MAPPER.writeValueAsString(filterPolicy)))
                .build())
        .get();

    // Initialize producers and consumers
    SnsConfig snsConfig =
        SnsConfig.builder()
            .topicArn(TOPIC_ARN)
            .region(System.getProperty(Constants.AWS_REGION))
            .endpoint(System.getProperty(Constants.SNS_ENDPOINT))
            .build();

    SqsConfig sqsConfig1 =
        SqsConfig.builder()
            .queueUrl(QUEUE_URL_1)
            .region(System.getProperty(Constants.AWS_REGION))
            .endpoint(System.getProperty(Constants.SQS_ENDPOINT))
            .build();

    SqsConfig sqsConfig2 =
        SqsConfig.builder()
            .queueUrl(QUEUE_URL_2)
            .region(System.getProperty(Constants.AWS_REGION))
            .endpoint(System.getProperty(Constants.SQS_ENDPOINT))
            .build();

    SNS_PRODUCER = new SnsProducer<>(snsConfig);
    SQS_CONSUMER_1 = new SqsConsumer(sqsConfig1);
    SQS_CONSUMER_2 = new SqsConsumer(sqsConfig2);
  }

  @AfterAll
  static void tearDown() {
    if (SNS_PRODUCER != null) {
      SNS_PRODUCER.close();
    }
    if (SQS_CONSUMER_1 != null) {
      SQS_CONSUMER_1.close();
    }
    if (SQS_CONSUMER_2 != null) {
      SQS_CONSUMER_2.close();
    }
    if (SNS_ASYNC_CLIENT != null) {
      SNS_ASYNC_CLIENT.close();
    }
    if (SQS_ASYNC_CLIENT != null) {
      SQS_ASYNC_CLIENT.close();
    }
  }

  @Test
  @SneakyThrows
  void testPublishToSnsAndReceiveFromSubscribedSqs() {
    // Arrange
    String testMessage = "test message from SNS";

    // Act
    SNS_PRODUCER.send(testMessage).get();

    // Wait for SNS -> SQS propagation
    List<Message> messages =
        await()
            .atMost(5, SECONDS)
            .until(() -> SQS_CONSUMER_1.receive().get(), msgs -> !msgs.isEmpty());
    SQS_CONSUMER_1.acknowledgeMessage(messages.get(0)).get();

    // Assert
    assertThat(messages).hasSize(1);

    // Parse SNS JSON wrapper
    String snsMessageBody = messages.get(0).getBody();
    JsonNode snsJson = OBJECT_MAPPER.readTree(snsMessageBody);

    assertThat(snsJson.has("Type")).isTrue();
    assertThat(snsJson.get("Type").asText()).isEqualTo("Notification");
    assertThat(snsJson.has("TopicArn")).isTrue();
    assertThat(snsJson.get("TopicArn").asText()).isEqualTo(TOPIC_ARN);
    assertThat(snsJson.has("Message")).isTrue();
    assertThat(snsJson.get("Message").asText()).isEqualTo(testMessage);
  }

  @Test
  @SneakyThrows
  void testPublishWithMessageAttributesFlowThroughSnsToSqs() {
    // Arrange
    String testMessage = "test message with attributes";
    Map<String, Object> attributes = new HashMap<>();
    attributes.put("priority", 10);
    attributes.put("type", "order");
    attributes.put("source", "api");

    // Act
    SNS_PRODUCER.send(testMessage, attributes).get();

    // Wait for SNS -> SQS propagation
    List<Message> messages =
        await()
            .atMost(5, SECONDS)
            .until(() -> SQS_CONSUMER_1.receive().get(), msgs -> !msgs.isEmpty());
    SQS_CONSUMER_1.acknowledgeMessage(messages.get(0)).get();

    // Assert
    assertThat(messages).hasSize(1);

    // Parse SNS JSON wrapper
    String snsMessageBody = messages.get(0).getBody();
    JsonNode snsJson = OBJECT_MAPPER.readTree(snsMessageBody);

    assertThat(snsJson.get("Message").asText()).isEqualTo(testMessage);

    // Verify message attributes from SNS
    assertThat(snsJson.has("MessageAttributes")).isTrue();
    JsonNode messageAttributes = snsJson.get("MessageAttributes");

    assertThat(messageAttributes.has("priority")).isTrue();
    assertThat(messageAttributes.get("priority").get("Type").asText()).isEqualTo("Number");
    assertThat(messageAttributes.get("priority").get("Value").asText()).isEqualTo("10");

    assertThat(messageAttributes.has("type")).isTrue();
    assertThat(messageAttributes.get("type").get("Type").asText()).isEqualTo("String");
    assertThat(messageAttributes.get("type").get("Value").asText()).isEqualTo("order");

    assertThat(messageAttributes.has("source")).isTrue();
    assertThat(messageAttributes.get("source").get("Type").asText()).isEqualTo("String");
    assertThat(messageAttributes.get("source").get("Value").asText()).isEqualTo("api");
  }

  @Test
  @SneakyThrows
  void testFilterPolicyRoutingBasedOnRoutingKeyAttribute() {
    // Arrange
    String messageForQueue2 = "message routed to queue 2";
    Map<String, Object> attributesForQueue2 = Map.of("routingKey", "queue2");

    String messageForQueue1 = "message for queue 1 (no filter)";
    Map<String, Object> attributesForQueue1 = Map.of("routingKey", "queue1");

    // Act - Send message with routingKey = "queue2"
    SNS_PRODUCER.send(messageForQueue2, attributesForQueue2).get();

    // Wait for SNS -> SQS propagation
    List<Message> messagesInQueue1 =
        await()
            .atMost(5, SECONDS)
            .until(() -> SQS_CONSUMER_1.receive().get(), msgs -> !msgs.isEmpty());
    List<Message> messagesInQueue2 =
        await()
            .atMost(5, SECONDS)
            .until(() -> SQS_CONSUMER_2.receive().get(), msgs -> !msgs.isEmpty());

    // Assert - Message with routingKey="queue2" should be in both queues
    // Queue1 has no filter (receives all)
    // Queue2 has filter for routingKey="queue2" (receives matching)
    assertThat(messagesInQueue1).hasSize(1);
    assertThat(messagesInQueue2).hasSize(1);

    // Clean up queue 1
    SQS_CONSUMER_1.acknowledgeMessage(messagesInQueue1.get(0)).get();

    // Verify message in queue 2
    String snsMessageBody = messagesInQueue2.get(0).getBody();
    JsonNode snsJson = OBJECT_MAPPER.readTree(snsMessageBody);
    assertThat(snsJson.get("Message").asText()).isEqualTo(messageForQueue2);
    SQS_CONSUMER_2.acknowledgeMessage(messagesInQueue2.get(0)).get();

    // Act - Send message with routingKey = "queue1"
    SNS_PRODUCER.send(messageForQueue1, attributesForQueue1).get();

    // Wait for SNS -> SQS propagation and verify routing
    messagesInQueue1 =
        await()
            .atMost(5, SECONDS)
            .until(() -> SQS_CONSUMER_1.receive().get(), msgs -> !msgs.isEmpty());
    messagesInQueue2 =
        await().atMost(5, SECONDS).until(() -> SQS_CONSUMER_2.receive().get(), List::isEmpty);

    // Assert - Message with routingKey="queue1" should only be in queue1
    // Queue1 has no filter (receives all)
    // Queue2 has filter for routingKey="queue2" (does NOT receive this)
    assertThat(messagesInQueue1).hasSize(1);
    assertThat(messagesInQueue2).isEmpty(); // Filtered out!

    // Verify message content in queue 1
    snsMessageBody = messagesInQueue1.get(0).getBody();
    snsJson = OBJECT_MAPPER.readTree(snsMessageBody);
    assertThat(snsJson.get("Message").asText()).isEqualTo(messageForQueue1);
    SQS_CONSUMER_1.acknowledgeMessage(messagesInQueue1.get(0)).get();
  }

  private static String getQueueArn(String queueUrl) throws Exception {
    return SQS_ASYNC_CLIENT
        .getQueueAttributes(
            GetQueueAttributesRequest.builder()
                .queueUrl(queueUrl)
                .attributeNames(QueueAttributeName.QUEUE_ARN)
                .build())
        .get()
        .attributes()
        .get(QueueAttributeName.QUEUE_ARN);
  }

  private static void setQueuePolicy(String queueUrl, String queueArn, String topicArn)
      throws Exception {
    String policy =
        "{"
            + "  \"Version\": \"2012-10-17\","
            + "  \"Statement\": [{"
            + "    \"Effect\": \"Allow\","
            + "    \"Principal\": \"*\","
            + "    \"Action\": \"sqs:SendMessage\","
            + "    \"Resource\": \""
            + queueArn
            + "\","
            + "    \"Condition\": {"
            + "      \"ArnEquals\": {"
            + "        \"aws:SourceArn\": \""
            + topicArn
            + "\""
            + "      }"
            + "    }"
            + "  }]"
            + "}";

    SQS_ASYNC_CLIENT
        .setQueueAttributes(
            SetQueueAttributesRequest.builder()
                .queueUrl(queueUrl)
                .attributes(Map.of(QueueAttributeName.POLICY, policy))
                .build())
        .get();
  }
}
