package com.streamforge.job.cdc;

import static org.assertj.core.api.Assertions.assertThat;

import com.streamforge.core.model.StreamEnvelop;
import com.streamforge.core.pipeline.PipelineBuilder;
import com.streamforge.pattern.dedup.Deduplicator;
import com.streamforge.pattern.filter.FilterInterceptor;
import com.streamforge.pattern.merge.StatefulMerger;
import com.streamforge.pattern.observability.*;
import com.streamforge.pattern.schema.SchemaEnforcer;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class MongoToKafkaJobTest {

  private static StreamEnvelop envelope(
      String source, String op, String primaryKey, String payloadJson, Instant eventTime) {
    return StreamEnvelop.builder()
        .operation(op)
        .source(source)
        .payloadJson(payloadJson)
        .primaryKey(primaryKey)
        .eventTime(eventTime)
        .processedTime(Instant.now())
        .metadata(new HashMap<>())
        .build();
  }

  /**
   * Replicates the MongoToKafkaJob pipeline chain using PipelineBuilder, starting from an
   * already-parsed StreamEnvelop stream (skipping MongoChangeStreamSource + parse).
   */
  private DataStream<StreamEnvelop> buildTestChain(DataStream<StreamEnvelop> input) {
    return PipelineBuilder.from(input)
        .parse(stream -> stream) // identity — source is already parsed
        .apply(new FlowDisruptionDetector<>(StreamEnvelop::getSource, Duration.ofMinutes(5)))
        .apply(new FilterInterceptor<>(e -> !"unknown".equals(e.getOperation())))
        .apply(
            new Deduplicator<>(
                e -> e.getPrimaryKey() + ":" + e.getEventTime(), Duration.ofMinutes(10)))
        .apply(
            new StatefulMerger<>(
                StreamEnvelop::getPrimaryKey,
                e -> {
                  Map<String, Object> map = new HashMap<>();
                  map.put("__op", e.getOperation());
                  Map<String, Object> payload = e.getPayloadAsMap();
                  if (payload != null) map.putAll(payload);
                  return map;
                },
                Set.of("updatedAt", "modifiedAt")))
        .apply(new SchemaEnforcer<>(StreamEnvelop::getPayloadAsMap, MongoToKafkaSchema.VERSIONS))
        .apply(new LatencyDetector<>(StreamEnvelop::getEventTime, Duration.ofSeconds(30)))
        .apply(
            new OnlineObserver<>(
                QualityCheck.of("null_payloads", e -> e.getPayloadJson() == null),
                QualityCheck.of("null_keys", e -> e.getPrimaryKey() == null)))
        .apply(new MetadataDecorator<>(StreamEnvelop::getMetadata, "pre-sink"))
        .process(new com.streamforge.job.cdc.processor.MongoToKafkaProcessor())
        .getStream();
  }

  @Test
  @DisplayName("valid events should pass through entire pattern chain")
  void validEventsShouldPassThroughEntireChain() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    Instant now = Instant.now();
    List<StreamEnvelop> data =
        List.of(
            envelope("coll-A", "INSERT", "key1", "{\"_id\":\"key1\",\"name\":\"Alice\"}", now),
            envelope(
                "coll-B",
                "UPDATE",
                "key2",
                "{\"_id\":\"key2\",\"name\":\"Bob\"}",
                now.plusMillis(100)));

    DataStream<StreamEnvelop> input =
        env.fromCollection(data, TypeInformation.of(StreamEnvelop.class));

    DataStream<StreamEnvelop> result = buildTestChain(input);

    List<StreamEnvelop> results = new ArrayList<>();
    try (CloseableIterator<StreamEnvelop> iter = result.executeAndCollect("test")) {
      while (iter.hasNext()) {
        results.add(iter.next());
      }
    }

    assertThat(results).hasSize(2);
    for (StreamEnvelop e : results) {
      assertThat(e.getTraceId()).isNotNull().startsWith("trace-");
      assertThat(e.getProcessedTime()).isNotNull();
      assertThat(e.getMetadata()).containsKey("stage.pre-sink.processedAt");
    }
  }

  @Test
  @DisplayName("unknown operations should be filtered out")
  void unknownOperationsShouldBeFilteredOut() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    Instant now = Instant.now();
    List<StreamEnvelop> data =
        List.of(
            envelope("coll-A", "INSERT", "key1", "{\"_id\":\"key1\"}", now),
            envelope("coll-A", "unknown", "key2", "{\"_id\":\"key2\"}", now.plusMillis(100)),
            envelope("coll-A", "UPDATE", "key3", "{\"_id\":\"key3\"}", now.plusMillis(200)));

    DataStream<StreamEnvelop> input =
        env.fromCollection(data, TypeInformation.of(StreamEnvelop.class));

    DataStream<StreamEnvelop> result = buildTestChain(input);

    List<StreamEnvelop> results = new ArrayList<>();
    try (CloseableIterator<StreamEnvelop> iter = result.executeAndCollect("test")) {
      while (iter.hasNext()) {
        results.add(iter.next());
      }
    }

    assertThat(results).hasSize(2);
    List<String> keys = results.stream().map(StreamEnvelop::getPrimaryKey).toList();
    assertThat(keys).containsExactlyInAnyOrder("key1", "key3");
  }

  @Test
  @DisplayName("events without _id should be dropped by schema enforcer")
  void eventsWithoutIdShouldBeDroppedBySchemaEnforcer() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    Instant now = Instant.now();
    List<StreamEnvelop> data =
        List.of(
            envelope("coll-A", "INSERT", "key1", "{\"_id\":\"key1\",\"name\":\"Alice\"}", now),
            envelope("coll-A", "INSERT", "key2", "{\"name\":\"Bob\"}", now.plusMillis(100)));

    DataStream<StreamEnvelop> input =
        env.fromCollection(data, TypeInformation.of(StreamEnvelop.class));

    DataStream<StreamEnvelop> result = buildTestChain(input);

    List<StreamEnvelop> results = new ArrayList<>();
    try (CloseableIterator<StreamEnvelop> iter = result.executeAndCollect("test")) {
      while (iter.hasNext()) {
        results.add(iter.next());
      }
    }

    assertThat(results).hasSize(1);
    assertThat(results.get(0).getPrimaryKey()).isEqualTo("key1");
  }
}
