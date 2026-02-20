package com.streamforge.job.route;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamforge.core.BaseIntegrationTest;
import com.streamforge.core.model.StreamEnvelop;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.http.HttpHost;
import org.apache.http.util.EntityUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.bson.Document;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.junit.jupiter.api.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@SuppressWarnings("resource")
public class EventRouterIntegrationTest extends BaseIntegrationTest {

  private static final ObjectMapper mapper = new ObjectMapper();
  private static RestClient esClient;

  @BeforeAll
  static void startJob() throws Exception {
    esClient =
        RestClient.builder(HttpHost.create("http://" + elasticsearch.getHttpHostAddress())).build();

    EventRouterJob job = new EventRouterJob();
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

  @AfterAll
  static void closeEsClient() {
    if (esClient != null) {
      try {
        esClient.close();
      } catch (Exception e) {
        System.err.println("[WARN] Failed to close ES client: " + e.getMessage());
      }
    }
  }

  @Test
  @Order(1)
  @DisplayName("orders source events should be routed to Elasticsearch")
  void ordersRouteToEs() throws Exception {
    String traceId = UUID.randomUUID().toString();
    sendStreamEvent("insert", "orders", Map.of("_id", 101, "name", "Order-A"), traceId);

    await()
        .atMost(Duration.ofSeconds(10))
        .pollInterval(Duration.ofSeconds(1))
        .untilAsserted(
            () -> {
              Map<String, Object> doc = getEsDocument(traceId);
              assertThat(doc).isNotNull();
              @SuppressWarnings("unchecked")
              Map<String, Object> source = (Map<String, Object>) doc.get("_source");
              assertThat(source).isNotNull();
              assertThat(source.get("source")).isEqualTo("orders");
            });
  }

  @Test
  @Order(2)
  @DisplayName("payments source events should be routed to Elasticsearch")
  void paymentsRouteToEs() throws Exception {
    String traceId = UUID.randomUUID().toString();
    sendStreamEvent("insert", "payments", Map.of("_id", 201, "name", "Payment-X"), traceId);

    await()
        .atMost(Duration.ofSeconds(10))
        .pollInterval(Duration.ofSeconds(1))
        .untilAsserted(
            () -> {
              Map<String, Object> doc = getEsDocument(traceId);
              assertThat(doc).isNotNull();
              @SuppressWarnings("unchecked")
              Map<String, Object> source = (Map<String, Object>) doc.get("_source");
              assertThat(source).isNotNull();
              assertThat(source.get("source")).isEqualTo("payments");
            });
  }

  @Test
  @Order(3)
  @DisplayName("unmatched source events should be routed to MongoDB")
  void unmatchedRouteToMongo() throws Exception {
    sendStreamEvent(
        "insert",
        "other-source",
        Map.of("_id", 301, "name", "Other-Event"),
        UUID.randomUUID().toString());
    Document doc = waitForMongoDocument(301, 10, "Other-Event");
    printAllDocs();
    assertThat(doc).isNotNull();
    assertThat(doc.getString("name")).isEqualTo("Other-Event");
  }

  private void sendStreamEvent(
      String op, String source, Map<String, Object> payload, String traceId) throws Exception {
    try (var producer =
        new KafkaProducer<String, String>(
            Map.of(
                "bootstrap.servers", kafka.getBootstrapServers(),
                "key.serializer", "org.apache.kafka.common.serialization.StringSerializer",
                "value.serializer", "org.apache.kafka.common.serialization.StringSerializer"))) {
      StreamEnvelop envelop = StreamEnvelop.of(op, source, payload, "_id");
      envelop.setTraceId(traceId);
      producer.send(new ProducerRecord<>(KAFKA_TOPIC_MAIN, envelop.toJson())).get();
    }
  }

  private Map<String, Object> getEsDocument(String docId) {
    try {
      Request request = new Request("GET", "/" + ES_INDEX + "/_doc/" + docId);
      Response response = esClient.performRequest(request);
      String body = EntityUtils.toString(response.getEntity());
      Map<String, Object> result = mapper.readValue(body, new TypeReference<>() {});
      return Boolean.TRUE.equals(result.get("found")) ? result : null;
    } catch (Exception e) {
      return null;
    }
  }
}
