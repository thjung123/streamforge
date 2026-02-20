package com.streamforge.job.cdc;

import static org.assertj.core.api.Assertions.assertThat;

import com.streamforge.core.BaseIntegrationTest;
import com.streamforge.core.model.StreamEnvelop;
import java.util.Map;
import java.util.UUID;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.bson.Document;
import org.junit.jupiter.api.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@SuppressWarnings("resource")
public class KafkaToMongoIntegrationTest extends BaseIntegrationTest {

  @BeforeAll
  static void startJob() throws Exception {
    KafkaToMongoJob job = new KafkaToMongoJob();
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
  void testInsertUpdateDeleteFlow() throws Exception {
    sendStreamEvent("insert", Map.of("_id", 1, "name", "Charlie"));
    Document inserted = waitForMongoDocument(1, 10, "Charlie");
    printAllDocs();
    assertThat(inserted).isNotNull();
    assertThat(inserted.getString("name")).isEqualTo("Charlie");

    sendStreamEvent("update", Map.of("_id", 1, "name", "Charlotte"));
    Document updated = waitForMongoDocument(1, 10, "Charlotte");
    printAllDocs();
    assertThat(updated).isNotNull();
    assertThat(updated.getString("name")).isEqualTo("Charlotte");

    sendStreamEvent("delete", Map.of("_id", 1));
    Document deleted = waitUntilDeleted(1, 10);
    printAllDocs();
    assertThat(deleted).isNull();
  }

  private void sendStreamEvent(String op, Map<String, Object> payload) throws Exception {
    try (var producer =
        new KafkaProducer<String, String>(
            Map.of(
                "bootstrap.servers", kafka.getBootstrapServers(),
                "key.serializer", "org.apache.kafka.common.serialization.StringSerializer",
                "value.serializer", "org.apache.kafka.common.serialization.StringSerializer"))) {
      StreamEnvelop envelop = StreamEnvelop.of(op, "test-source", payload, "_id");
      envelop.setTraceId(UUID.randomUUID().toString());
      producer.send(new ProducerRecord<>(KAFKA_TOPIC_MAIN, envelop.toJson())).get();
    }
  }
}
