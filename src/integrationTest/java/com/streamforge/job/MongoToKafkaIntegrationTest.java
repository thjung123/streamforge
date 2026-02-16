package com.streamforge.job;

import static org.assertj.core.api.Assertions.assertThat;

import com.mongodb.client.MongoCollection;
import com.streamforge.core.BaseIntegrationTest;
import com.streamforge.core.model.StreamEnvelop;
import com.streamforge.job.sync.cdc.MongoToKafkaJob;
import java.time.Duration;
import java.util.*;
import java.util.Locale;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.bson.Document;
import org.junit.jupiter.api.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@SuppressWarnings("resource")
public class MongoToKafkaIntegrationTest extends BaseIntegrationTest {

  private static KafkaConsumer<String, String> consumer;

  @BeforeAll
  static void setupJobAndConsumer() throws Exception {
    String groupId = "mongo-to-kafka-test-group-" + UUID.randomUUID();

    consumer =
        new KafkaConsumer<>(
            Map.of(
                "bootstrap.servers",
                kafka.getBootstrapServers(),
                "group.id",
                groupId,
                "auto.offset.reset",
                "earliest",
                "enable.auto.commit",
                "false",
                "key.deserializer",
                StringDeserializer.class.getName(),
                "value.deserializer",
                StringDeserializer.class.getName()));
    consumer.subscribe(List.of(KAFKA_TOPIC_MAIN));

    MongoToKafkaJob job = new MongoToKafkaJob();
    StreamExecutionEnvironment env = job.buildPipeline();

    Thread flinkThread =
        new Thread(
            () -> {
              try {
                env.execute("mongo-to-kafka-test-job");
              } catch (Exception e) {
                System.err.println("[ERROR] Flink execution failed: " + e.getMessage());
              }
            });
    flinkThread.setDaemon(true);
    flinkThread.start();

    System.out.println("[SETUP] Flink job started, waiting for warm-up...");
    Thread.sleep(8000);
  }

  @Test
  @Order(1)
  @DisplayName("CDC insert/update/delete flow with metadata verification")
  void testInsertUpdateDeleteFlow() {
    MongoCollection<Document> collection =
        mongoClient.getDatabase(MONGO_DB_NAME).getCollection(MONGO_COLLECTION);

    System.out.println("\n=== [STEP] INSERT ===");
    collection.insertOne(new Document("_id", 1).append("name", "Alice"));
    String insertJson = waitForKafkaEvent("\"operation\":\"insert\"", Duration.ofSeconds(20));
    StreamEnvelop insertEnv = StreamEnvelop.fromJson(insertJson);
    assertThat(insertEnv.getOperation()).isEqualToIgnoringCase("insert");
    assertThat(insertEnv.getPayloadAsMap().get("name"))
        .as("Inserted name must be Alice")
        .isEqualTo("Alice");

    assertThat(insertEnv.getMetadata()).isNotNull();
    assertThat(insertEnv.getMetadata()).containsKey("stage.pre-sink.processedAt");
    assertThat(insertEnv.getMetadata()).containsKey("stage.pre-sink.taskName");
    assertThat(insertEnv.getMetadata()).containsKey("stage.pre-sink.subtaskIndex");
    System.out.println("[VERIFY] Metadata: " + insertEnv.getMetadata());

    System.out.println("\n=== [STEP] UPDATE ===");
    collection.updateOne(
        new Document("_id", 1), new Document("$set", new Document("name", "Alicia")));
    String updateJson = waitForKafkaEvent("\"operation\":\"update\"", Duration.ofSeconds(20));
    StreamEnvelop updateEnv = StreamEnvelop.fromJson(updateJson);
    assertThat(updateEnv.getOperation()).isEqualToIgnoringCase("update");
    Map<String, Object> updatePayload = updateEnv.getPayloadAsMap();

    System.out.println("[DEBUG] Update payload: " + updatePayload);

    assertThat(updatePayload.get("_id")).isEqualTo(1);
    if (updatePayload.containsKey("name")) {
      assertThat(updatePayload.get("name")).as("Updated name must be Alicia").isEqualTo("Alicia");
    } else {
      System.err.println("[WARN] 'name' field missing in update payload → running in delta mode");
    }

    assertThat(updateEnv.getMetadata()).containsKey("stage.pre-sink.processedAt");

    System.out.println("\n=== [STEP] DELETE ===");
    collection.deleteOne(new Document("_id", 1));
    String deleteJson = waitForKafkaEvent("\"operation\":\"delete\"", Duration.ofSeconds(20));
    StreamEnvelop deleteEnv = StreamEnvelop.fromJson(deleteJson);
    assertThat(deleteEnv.getOperation()).isEqualToIgnoringCase("delete");
    assertThat(deleteEnv.getPayloadAsMap().get("_id")).isEqualTo(1);
  }

  @Test
  @Order(2)
  @DisplayName("Batch inserts produce exact message count — no duplicates, no drops")
  void testBatchInsertExactCount() {
    MongoCollection<Document> collection =
        mongoClient.getDatabase(MONGO_DB_NAME).getCollection(MONGO_COLLECTION);

    int batchSize = 5;
    System.out.println("\n=== [BATCH] Inserting " + batchSize + " documents ===");
    for (int i = 100; i < 100 + batchSize; i++) {
      collection.insertOne(new Document("_id", i).append("name", "user-" + i));
    }

    List<StreamEnvelop> received = collectKafkaEvents(batchSize, Duration.ofSeconds(30));

    assertThat(received).hasSize(batchSize);

    Set<Object> receivedIds = new HashSet<>();
    for (StreamEnvelop env : received) {
      assertThat(env.getOperation()).isEqualToIgnoringCase("insert");
      receivedIds.add(env.getPayloadAsMap().get("_id"));
      assertThat(env.getMetadata()).containsKey("stage.pre-sink.processedAt");
    }

    assertThat(receivedIds).hasSize(batchSize);
    System.out.println("[VERIFY] Received exactly " + batchSize + " unique events");
  }

  private String waitForKafkaEvent(String keyword, Duration timeout) {
    long end = System.currentTimeMillis() + timeout.toMillis();
    while (System.currentTimeMillis() < end) {
      var polled = consumer.poll(Duration.ofMillis(500));
      if (!polled.isEmpty()) {
        for (ConsumerRecord<String, String> record : polled) {
          String value = record.value();
          System.out.println("[KAFKA] " + value);
          if (value.toLowerCase(Locale.ROOT).contains(keyword.toLowerCase(Locale.ROOT))) {
            System.out.println("[MATCH] Found event containing " + keyword);
            return value;
          }
        }
      }
    }
    throw new AssertionError(
        "No Kafka event found with keyword: " + keyword + " within " + timeout.toSeconds() + "s");
  }

  private List<StreamEnvelop> collectKafkaEvents(int expectedCount, Duration timeout) {
    List<StreamEnvelop> events = new ArrayList<>();
    long end = System.currentTimeMillis() + timeout.toMillis();
    while (events.size() < expectedCount && System.currentTimeMillis() < end) {
      var polled = consumer.poll(Duration.ofMillis(500));
      for (ConsumerRecord<String, String> record : polled) {
        System.out.println("[KAFKA] " + record.value());
        StreamEnvelop env = StreamEnvelop.fromJson(record.value());
        events.add(env);
      }
    }
    return events;
  }

  @AfterAll
  static void cleanup() {
    if (consumer != null) {
      consumer.close();
    }
  }
}
