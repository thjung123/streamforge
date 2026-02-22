package com.streamforge.job.materialize;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.streamforge.core.BaseIntegrationTest;
import com.streamforge.core.model.StreamEnvelop;
import com.streamforge.core.util.JsonUtils;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@SuppressWarnings("resource")
public class UserStateMaterializeIntegrationTest extends BaseIntegrationTest {

  private static final String CHANGELOG_TOPIC = "user-changelog";
  private static final List<StreamEnvelop> received = new CopyOnWriteArrayList<>();

  @BeforeAll
  static void startJob() throws Exception {
    createKafkaTopic(CHANGELOG_TOPIC);

    System.setProperty("STATE_TTL_HOURS", "1");
    System.setProperty("CHANGELOG_TOPIC", CHANGELOG_TOPIC);

    UserStateMaterializeJob job = new UserStateMaterializeJob();
    StreamExecutionEnvironment env = job.buildPipeline();
    Thread flinkThread =
        new Thread(
            () -> {
              try {
                env.execute();
              } catch (Exception e) {
                System.err.println("[ERROR] Flink execution failed: " + e.getMessage());
              }
            });
    flinkThread.setDaemon(true);
    flinkThread.start();

    startChangelogConsumer();

    Thread.sleep(3000);
  }

  private static void startChangelogConsumer() {
    Thread consumer =
        new Thread(
            () -> {
              try (var kafkaConsumer =
                  new KafkaConsumer<String, String>(
                      Map.of(
                          "bootstrap.servers", kafka.getBootstrapServers(),
                          "group.id", "test-changelog-consumer",
                          "auto.offset.reset", "earliest",
                          "key.deserializer",
                              "org.apache.kafka.common.serialization.StringDeserializer",
                          "value.deserializer",
                              "org.apache.kafka.common.serialization.StringDeserializer"))) {
                kafkaConsumer.subscribe(List.of(CHANGELOG_TOPIC));
                while (!Thread.currentThread().isInterrupted()) {
                  ConsumerRecords<String, String> records =
                      kafkaConsumer.poll(Duration.ofMillis(500));
                  records.forEach(
                      r -> {
                        StreamEnvelop envelop = JsonUtils.fromJson(r.value(), StreamEnvelop.class);
                        received.add(envelop);
                      });
                }
              }
            });
    consumer.setDaemon(true);
    consumer.start();
  }

  @Test
  @Order(1)
  @DisplayName("should emit INSERT changelog to Kafka for new key")
  void insertChangelog() throws Exception {
    int before = received.size();
    sendEvent("mat-user-1", "CREATE", "{\"name\":\"Alice\",\"age\":30}");

    await()
        .atMost(Duration.ofSeconds(15))
        .pollInterval(Duration.ofSeconds(1))
        .untilAsserted(
            () -> {
              List<StreamEnvelop> newEvents = received.subList(before, received.size());
              assertThat(newEvents).anyMatch(e -> "CHANGELOG_INSERT".equals(e.getOperation()));

              StreamEnvelop insert =
                  newEvents.stream()
                      .filter(e -> "CHANGELOG_INSERT".equals(e.getOperation()))
                      .findFirst()
                      .orElseThrow();
              Map<String, Object> payload = insert.getPayloadAsMap();
              assertThat(payload.get("changeType")).isEqualTo("INSERT");
              assertThat(payload.get("after")).isNotNull();
              assertThat(payload.get("before")).isNull();
            });
  }

  @Test
  @Order(2)
  @DisplayName("should emit UPDATE changelog to Kafka for existing key")
  void updateChangelog() throws Exception {
    int before = received.size();
    sendEvent("mat-user-1", "UPDATE", "{\"name\":\"Alice\",\"age\":31}");

    await()
        .atMost(Duration.ofSeconds(15))
        .pollInterval(Duration.ofSeconds(1))
        .untilAsserted(
            () -> {
              List<StreamEnvelop> newEvents = received.subList(before, received.size());
              assertThat(newEvents).anyMatch(e -> "CHANGELOG_UPDATE".equals(e.getOperation()));

              StreamEnvelop update =
                  newEvents.stream()
                      .filter(e -> "CHANGELOG_UPDATE".equals(e.getOperation()))
                      .findFirst()
                      .orElseThrow();
              Map<String, Object> payload = update.getPayloadAsMap();
              assertThat(payload.get("changeType")).isEqualTo("UPDATE");
              assertThat(payload.get("before")).isNotNull();
              assertThat(payload.get("after")).isNotNull();
            });
  }

  @Test
  @Order(3)
  @DisplayName("should emit DELETE changelog to Kafka when delete predicate matches")
  void deleteChangelog() throws Exception {
    int before = received.size();
    sendEvent("mat-user-1", "DELETE", "{\"name\":\"Alice\",\"age\":31}");

    await()
        .atMost(Duration.ofSeconds(15))
        .pollInterval(Duration.ofSeconds(1))
        .untilAsserted(
            () -> {
              List<StreamEnvelop> newEvents = received.subList(before, received.size());
              assertThat(newEvents).anyMatch(e -> "CHANGELOG_DELETE".equals(e.getOperation()));

              StreamEnvelop delete =
                  newEvents.stream()
                      .filter(e -> "CHANGELOG_DELETE".equals(e.getOperation()))
                      .findFirst()
                      .orElseThrow();
              Map<String, Object> payload = delete.getPayloadAsMap();
              assertThat(payload.get("changeType")).isEqualTo("DELETE");
              assertThat(payload.get("before")).isNotNull();
              assertThat(payload.get("after")).isNull();
            });
  }

  @Test
  @Order(4)
  @DisplayName("should emit re-INSERT changelog after DELETE for same key")
  void reInsertAfterDelete() throws Exception {
    int before = received.size();
    sendEvent("mat-user-1", "CREATE", "{\"name\":\"Bob\",\"age\":25}");

    await()
        .atMost(Duration.ofSeconds(15))
        .pollInterval(Duration.ofSeconds(1))
        .untilAsserted(
            () -> {
              List<StreamEnvelop> newEvents = received.subList(before, received.size());
              assertThat(newEvents).anyMatch(e -> "CHANGELOG_INSERT".equals(e.getOperation()));
            });
  }

  private void sendEvent(String userId, String operation, String payloadJson) throws Exception {
    try (var producer =
        new KafkaProducer<String, String>(
            Map.of(
                "bootstrap.servers", kafka.getBootstrapServers(),
                "key.serializer", "org.apache.kafka.common.serialization.StringSerializer",
                "value.serializer", "org.apache.kafka.common.serialization.StringSerializer"))) {
      StreamEnvelop envelop =
          StreamEnvelop.builder()
              .operation(operation)
              .source("user-service")
              .payloadJson(payloadJson)
              .primaryKey(userId)
              .eventTime(Instant.now())
              .processedTime(Instant.now())
              .traceId(UUID.randomUUID().toString())
              .build();
      producer.send(new ProducerRecord<>(KAFKA_TOPIC_MAIN, envelop.toJson())).get();
    }
  }
}
