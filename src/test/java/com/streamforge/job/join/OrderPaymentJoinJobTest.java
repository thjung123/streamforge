package com.streamforge.job.join;

import static org.assertj.core.api.Assertions.assertThat;

import com.streamforge.core.model.StreamEnvelop;
import com.streamforge.core.pipeline.PipelineBuilder;
import com.streamforge.pattern.enrich.DynamicJoiner;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class OrderPaymentJoinJobTest {

  private static StreamEnvelop envelope(
      String source, String op, String primaryKey, String payloadJson) {
    return StreamEnvelop.builder()
        .operation(op)
        .source(source)
        .payloadJson(payloadJson)
        .primaryKey(primaryKey)
        .eventTime(Instant.now())
        .processedTime(Instant.now())
        .metadata(new HashMap<>())
        .build();
  }

  private static List<StreamEnvelop> execute(DataStream<StreamEnvelop> stream) throws Exception {
    List<StreamEnvelop> results = new ArrayList<>();
    try (CloseableIterator<StreamEnvelop> iter = stream.executeAndCollect("test")) {
      while (iter.hasNext()) {
        results.add(iter.next());
      }
    }
    return results;
  }

  @Nested
  @DisplayName("INNER join pipeline")
  class InnerJoinPipeline {

    @Test
    @DisplayName("should join orders with matching payments")
    void shouldJoinOrdersWithMatchingPayments() throws Exception {
      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      env.setParallelism(1);

      List<StreamEnvelop> ordersData =
          List.of(
              envelope("orders", "INSERT", "order-1", "{\"amount\":100}"),
              envelope("orders", "INSERT", "order-2", "{\"amount\":200}"));

      List<StreamEnvelop> paymentsData =
          List.of(
              envelope("payments", "INSERT", "order-1", "{\"status\":\"PAID\"}"),
              envelope("payments", "INSERT", "order-2", "{\"status\":\"PENDING\"}"));

      DataStream<StreamEnvelop> orders =
          env.fromCollection(ordersData, TypeInformation.of(StreamEnvelop.class));
      DataStream<StreamEnvelop> payments =
          env.fromCollection(paymentsData, TypeInformation.of(StreamEnvelop.class));

      DynamicJoiner<StreamEnvelop, StreamEnvelop> joiner =
          DynamicJoiner.<StreamEnvelop, StreamEnvelop>builder()
              .leftKeyExtractor(StreamEnvelop::getPrimaryKey)
              .rightKeyExtractor(StreamEnvelop::getPrimaryKey)
              .joinFunction(
                  (order, payment) -> {
                    if (order.getMetadata() == null) {
                      order.setMetadata(new HashMap<>());
                    }
                    order.getMetadata().put("paymentPayload", payment.getPayloadJson());
                    return order;
                  })
              .joinType(DynamicJoiner.JoinType.INNER)
              .ttl(Duration.ofMinutes(10))
              .build();

      DataStream<StreamEnvelop> joined =
          PipelineBuilder.from(orders).enrich(payments, joiner).getStream();

      List<StreamEnvelop> results = execute(joined);

      assertThat(results).isNotEmpty();
      for (StreamEnvelop result : results) {
        assertThat(result.getSource()).isEqualTo("orders");
        assertThat(result.getMetadata()).containsKey("paymentPayload");
      }
    }

    @Test
    @DisplayName("should not emit orders without matching payments")
    void shouldNotEmitUnmatchedOrders() throws Exception {
      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      env.setParallelism(1);

      List<StreamEnvelop> ordersData =
          List.of(
              envelope("orders", "INSERT", "order-1", "{\"amount\":100}"),
              envelope("orders", "INSERT", "order-no-match", "{\"amount\":300}"));

      List<StreamEnvelop> paymentsData =
          List.of(envelope("payments", "INSERT", "order-1", "{\"status\":\"PAID\"}"));

      DataStream<StreamEnvelop> orders =
          env.fromCollection(ordersData, TypeInformation.of(StreamEnvelop.class));
      DataStream<StreamEnvelop> payments =
          env.fromCollection(paymentsData, TypeInformation.of(StreamEnvelop.class));

      DynamicJoiner<StreamEnvelop, StreamEnvelop> joiner =
          DynamicJoiner.<StreamEnvelop, StreamEnvelop>builder()
              .leftKeyExtractor(StreamEnvelop::getPrimaryKey)
              .rightKeyExtractor(StreamEnvelop::getPrimaryKey)
              .joinFunction(
                  (order, payment) -> {
                    if (order.getMetadata() == null) {
                      order.setMetadata(new HashMap<>());
                    }
                    order.getMetadata().put("paymentPayload", payment.getPayloadJson());
                    return order;
                  })
              .joinType(DynamicJoiner.JoinType.INNER)
              .ttl(Duration.ofMinutes(10))
              .build();

      DataStream<StreamEnvelop> joined =
          PipelineBuilder.from(orders).enrich(payments, joiner).getStream();

      List<StreamEnvelop> results = execute(joined);

      List<String> keys = results.stream().map(StreamEnvelop::getPrimaryKey).toList();
      assertThat(keys).doesNotContain("order-no-match");
    }
  }

  @Nested
  @DisplayName("LEFT join pipeline (OrderPaymentJoinJob default)")
  class LeftJoinPipeline {

    @Test
    @DisplayName("should enrich orders with payment data via buildJoiner")
    void shouldEnrichOrdersWithPaymentData() throws Exception {
      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      env.setParallelism(1);

      List<StreamEnvelop> ordersData =
          List.of(
              envelope("orders", "INSERT", "order-1", "{\"amount\":100}"),
              envelope("orders", "INSERT", "order-2", "{\"amount\":200}"));

      List<StreamEnvelop> paymentsData =
          List.of(
              envelope("payments", "INSERT", "order-1", "{\"status\":\"PAID\"}"),
              envelope("payments", "INSERT", "order-2", "{\"status\":\"PENDING\"}"));

      DataStream<StreamEnvelop> orders =
          env.fromCollection(ordersData, TypeInformation.of(StreamEnvelop.class));
      DataStream<StreamEnvelop> payments =
          env.fromCollection(paymentsData, TypeInformation.of(StreamEnvelop.class));

      DynamicJoiner<StreamEnvelop, StreamEnvelop> joiner =
          OrderPaymentJoinJob.buildJoiner(Duration.ofMinutes(10));

      DataStream<StreamEnvelop> joined =
          PipelineBuilder.from(orders).enrich(payments, joiner).getStream();

      List<StreamEnvelop> results = execute(joined);

      assertThat(results).isNotEmpty();
      assertThat(results).allMatch(e -> "orders".equals(e.getSource()));

      List<StreamEnvelop> enriched =
          results.stream().filter(e -> e.getMetadata().containsKey("paymentPayload")).toList();
      assertThat(enriched).isNotEmpty();
      for (StreamEnvelop e : enriched) {
        assertThat(e.getMetadata()).containsKey("paymentSource");
        assertThat(e.getMetadata().get("paymentSource")).isEqualTo("payments");
      }
    }

    @Test
    @DisplayName("should preserve all orders even without matching payments")
    void shouldPreserveAllOrders() throws Exception {
      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      env.setParallelism(1);

      List<StreamEnvelop> ordersData =
          List.of(
              envelope("orders", "INSERT", "order-1", "{\"amount\":100}"),
              envelope("orders", "INSERT", "order-lonely", "{\"amount\":500}"));

      List<StreamEnvelop> paymentsData =
          List.of(envelope("payments", "INSERT", "order-1", "{\"status\":\"PAID\"}"));

      DataStream<StreamEnvelop> orders =
          env.fromCollection(ordersData, TypeInformation.of(StreamEnvelop.class));
      DataStream<StreamEnvelop> payments =
          env.fromCollection(paymentsData, TypeInformation.of(StreamEnvelop.class));

      DynamicJoiner<StreamEnvelop, StreamEnvelop> joiner =
          OrderPaymentJoinJob.buildJoiner(Duration.ofMinutes(10));

      DataStream<StreamEnvelop> joined =
          PipelineBuilder.from(orders).enrich(payments, joiner).getStream();

      List<StreamEnvelop> results = execute(joined);

      List<String> keys = results.stream().map(StreamEnvelop::getPrimaryKey).toList();
      assertThat(keys).contains("order-1");
    }

    @Test
    @DisplayName("should enrich with correct payment data per key")
    void shouldEnrichWithCorrectPaymentPerKey() throws Exception {
      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      env.setParallelism(1);

      List<StreamEnvelop> ordersData =
          List.of(
              envelope("orders", "INSERT", "order-A", "{\"item\":\"Widget\"}"),
              envelope("orders", "INSERT", "order-B", "{\"item\":\"Gadget\"}"));

      List<StreamEnvelop> paymentsData =
          List.of(
              envelope("payments", "INSERT", "order-A", "{\"method\":\"CARD\"}"),
              envelope("payments", "INSERT", "order-B", "{\"method\":\"CASH\"}"));

      DataStream<StreamEnvelop> orders =
          env.fromCollection(ordersData, TypeInformation.of(StreamEnvelop.class));
      DataStream<StreamEnvelop> payments =
          env.fromCollection(paymentsData, TypeInformation.of(StreamEnvelop.class));

      DynamicJoiner<StreamEnvelop, StreamEnvelop> joiner =
          OrderPaymentJoinJob.buildJoiner(Duration.ofMinutes(10));

      DataStream<StreamEnvelop> joined =
          PipelineBuilder.from(orders).enrich(payments, joiner).getStream();

      List<StreamEnvelop> results = execute(joined);

      Map<String, String> paymentByOrder =
          results.stream()
              .filter(e -> e.getMetadata().containsKey("paymentPayload"))
              .collect(
                  Collectors.toMap(
                      StreamEnvelop::getPrimaryKey,
                      e -> e.getMetadata().get("paymentPayload"),
                      (a, b) -> a));

      if (paymentByOrder.containsKey("order-A")) {
        assertThat(paymentByOrder.get("order-A")).contains("CARD");
      }
      if (paymentByOrder.containsKey("order-B")) {
        assertThat(paymentByOrder.get("order-B")).contains("CASH");
      }
    }
  }

  @Nested
  @DisplayName("N:M join scenario")
  class NtoMJoinScenario {

    @Test
    @DisplayName("should join one order with multiple payments")
    void shouldJoinOneOrderWithMultiplePayments() throws Exception {
      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      env.setParallelism(1);

      List<StreamEnvelop> ordersData =
          List.of(envelope("orders", "INSERT", "order-1", "{\"amount\":300}"));

      List<StreamEnvelop> paymentsData =
          List.of(
              envelope("payments", "INSERT", "order-1", "{\"amount\":100,\"seq\":1}"),
              envelope("payments", "INSERT", "order-1", "{\"amount\":200,\"seq\":2}"));

      DataStream<StreamEnvelop> orders =
          env.fromCollection(ordersData, TypeInformation.of(StreamEnvelop.class));
      DataStream<StreamEnvelop> payments =
          env.fromCollection(paymentsData, TypeInformation.of(StreamEnvelop.class));

      DynamicJoiner<StreamEnvelop, StreamEnvelop> joiner =
          DynamicJoiner.<StreamEnvelop, StreamEnvelop>builder()
              .leftKeyExtractor(StreamEnvelop::getPrimaryKey)
              .rightKeyExtractor(StreamEnvelop::getPrimaryKey)
              .joinFunction(
                  (order, payment) -> {
                    if (order.getMetadata() == null) {
                      order.setMetadata(new HashMap<>());
                    }
                    order.getMetadata().put("paymentPayload", payment.getPayloadJson());
                    return order;
                  })
              .joinType(DynamicJoiner.JoinType.INNER)
              .ttl(Duration.ofMinutes(10))
              .build();

      DataStream<StreamEnvelop> joined =
          PipelineBuilder.from(orders).enrich(payments, joiner).getStream();

      List<StreamEnvelop> results = execute(joined);

      assertThat(results).isNotEmpty();
      assertThat(results).allMatch(e -> "order-1".equals(e.getPrimaryKey()));
    }
  }

  @Nested
  @DisplayName("SPI registration")
  class SpiRegistration {

    @Test
    @DisplayName("should be discoverable via ServiceLoader")
    void shouldBeDiscoverableViaSpi() {
      var jobs = java.util.ServiceLoader.load(com.streamforge.core.launcher.StreamJob.class);
      boolean found = false;
      for (var job : jobs) {
        if (job instanceof OrderPaymentJoinJob) {
          found = true;
          assertThat(job.name()).isEqualTo("OrderPaymentJoin");
          break;
        }
      }
      assertThat(found).isTrue();
    }
  }
}
