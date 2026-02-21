package com.streamforge.job.session;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.streamforge.core.BaseIntegrationTest;
import com.streamforge.core.model.StreamEnvelop;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.bson.Document;
import org.junit.jupiter.api.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@SuppressWarnings("resource")
public class UserSessionAnalysisIntegrationTest extends BaseIntegrationTest {

  @BeforeAll
  static void startJob() throws Exception {
    System.setProperty("SESSION_GAP_SECONDS", "5");

    UserSessionAnalysisJob job = new UserSessionAnalysisJob();
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
  @DisplayName("should produce session result in MongoDB after session closes")
  void sessionClosedAndWrittenToMongo() throws Exception {
    Instant base = Instant.now();

    sendEvent("user-session-1", "CLICK", base);
    sendEvent("user-session-1", "SCROLL", base.plusMillis(1000));
    sendEvent("user-session-1", "BUY", base.plusMillis(2000));

    Thread.sleep(1000);

    sendEvent("trigger", "TRIGGER", base.plusMillis(60000));

    await()
        .atMost(Duration.ofSeconds(15))
        .pollInterval(Duration.ofSeconds(1))
        .untilAsserted(
            () -> {
              Document doc = waitForMongoDocument("user-session-1", 1, null);
              assertThat(doc).isNotNull();
              assertThat(doc.getString("actions")).isEqualTo("CLICK,SCROLL,BUY");
              assertThat(doc.getInteger("count")).isEqualTo(3);
            });
  }

  @Test
  @Order(2)
  @DisplayName("should produce separate sessions when gap exceeded")
  void separateSessionsOnGap() throws Exception {
    Instant base = Instant.now().plusMillis(100000);

    sendEvent("user-session-2", "VIEW", base);
    sendEvent("user-session-2", "CLICK", base.plusMillis(1000));

    Thread.sleep(1000);

    sendEvent("trigger2", "TRIGGER", base.plusMillis(60000));

    await()
        .atMost(Duration.ofSeconds(15))
        .pollInterval(Duration.ofSeconds(1))
        .untilAsserted(
            () -> {
              Document doc = waitForMongoDocument("user-session-2", 1, null);
              assertThat(doc).isNotNull();
              assertThat(doc.getString("actions")).isEqualTo("VIEW,CLICK");
              assertThat(doc.getInteger("count")).isEqualTo(2);
            });
  }

  private void sendEvent(String userId, String operation, Instant eventTime) throws Exception {
    try (var producer =
        new KafkaProducer<String, String>(
            Map.of(
                "bootstrap.servers", kafka.getBootstrapServers(),
                "key.serializer", "org.apache.kafka.common.serialization.StringSerializer",
                "value.serializer", "org.apache.kafka.common.serialization.StringSerializer"))) {
      StreamEnvelop envelop =
          StreamEnvelop.builder()
              .operation(operation)
              .source("clickstream")
              .payloadJson("{}")
              .primaryKey(userId)
              .eventTime(eventTime)
              .processedTime(Instant.now())
              .traceId(UUID.randomUUID().toString())
              .build();
      producer.send(new ProducerRecord<>(KAFKA_TOPIC_MAIN, envelop.toJson())).get();
    }
  }
}
