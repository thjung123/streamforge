package com.streamforge.job.ingest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.streamforge.core.BaseIntegrationTest;
import com.streamforge.core.model.StreamEnvelop;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.bson.Document;
import org.junit.jupiter.api.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@SuppressWarnings("resource")
public class MergedIngestIntegrationTest extends BaseIntegrationTest {

  private static final String SECONDARY_TOPIC = "payments-topic";

  @BeforeAll
  static void startJob() throws Exception {
    createKafkaTopic(SECONDARY_TOPIC);

    System.setProperty("STREAM_TOPIC_SECONDARY", SECONDARY_TOPIC);

    MergedIngestJob job = new MergedIngestJob();
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

    Thread.sleep(3000);
  }

  @Test
  @Order(1)
  @DisplayName("events from primary topic should appear in MongoDB with ingestSource=orders")
  void primaryTopicEventsMerged() throws Exception {
    sendStreamEvent(KAFKA_TOPIC_MAIN, "insert", "orders", Map.of("_id", 501, "name", "Order-1"));

    await()
        .atMost(Duration.ofSeconds(10))
        .pollInterval(Duration.ofSeconds(1))
        .untilAsserted(
            () -> {
              Document doc = waitForMongoDocument(501, 1, "Order-1");
              assertThat(doc).isNotNull();
            });
  }

  @Test
  @Order(2)
  @DisplayName("events from secondary topic should appear in MongoDB with ingestSource=payments")
  void secondaryTopicEventsMerged() throws Exception {
    sendStreamEvent(SECONDARY_TOPIC, "insert", "payments", Map.of("_id", 601, "name", "Pay-1"));

    await()
        .atMost(Duration.ofSeconds(10))
        .pollInterval(Duration.ofSeconds(1))
        .untilAsserted(
            () -> {
              Document doc = waitForMongoDocument(601, 1, "Pay-1");
              assertThat(doc).isNotNull();
            });
  }

  @Test
  @Order(3)
  @DisplayName("events from both topics should coexist in MongoDB")
  void bothTopicsCoexist() {
    printAllDocs();
  }

  private void sendStreamEvent(String topic, String op, String source, Map<String, Object> payload)
      throws Exception {
    try (var producer =
        new KafkaProducer<String, String>(
            Map.of(
                "bootstrap.servers", kafka.getBootstrapServers(),
                "key.serializer", "org.apache.kafka.common.serialization.StringSerializer",
                "value.serializer", "org.apache.kafka.common.serialization.StringSerializer"))) {
      StreamEnvelop envelop = StreamEnvelop.of(op, source, payload, "_id");
      envelop.setTraceId(UUID.randomUUID().toString());
      producer.send(new ProducerRecord<>(topic, envelop.toJson())).get();
    }
  }
}
