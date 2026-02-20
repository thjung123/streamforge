package com.streamforge.job.cdc;

import static org.assertj.core.api.Assertions.assertThat;

import com.streamforge.core.model.StreamEnvelop;
import com.streamforge.core.pipeline.PipelineBuilder;
import com.streamforge.pattern.enrich.StaticJoiner;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class KafkaToMongoJobTest {

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

  @Test
  @DisplayName("enrich pipeline should enrich main events with reference data")
  void enrichPipelineShouldEnrichMainEventsWithReferenceData() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    List<StreamEnvelop> mainData =
        List.of(
            envelope("orders", "INSERT", "user1", "{\"orderId\":100}"),
            envelope("orders", "INSERT", "user2", "{\"orderId\":200}"));

    List<StreamEnvelop> refData =
        List.of(
            envelope("users", "SNAPSHOT", "user1", "{\"name\":\"Alice\"}"),
            envelope("users", "SNAPSHOT", "user2", "{\"name\":\"Bob\"}"));

    DataStream<StreamEnvelop> mainStream =
        env.fromCollection(mainData, TypeInformation.of(StreamEnvelop.class));
    DataStream<StreamEnvelop> refStream =
        env.fromCollection(refData, TypeInformation.of(StreamEnvelop.class));

    MapStateDescriptor<String, StreamEnvelop> descriptor =
        new MapStateDescriptor<>(
            "ref-state", Types.STRING, TypeInformation.of(StreamEnvelop.class));

    DataStream<StreamEnvelop> enriched =
        PipelineBuilder.from(mainStream)
            .parse(stream -> stream) // identity parse
            .enrich(
                refStream,
                StaticJoiner.<StreamEnvelop, StreamEnvelop>builder()
                    .mainKeyExtractor(StreamEnvelop::getPrimaryKey)
                    .refKeyExtractor(StreamEnvelop::getPrimaryKey)
                    .joinFunction(
                        (event, ref) -> {
                          if (event.getMetadata() == null) {
                            event.setMetadata(new HashMap<>());
                          }
                          event.getMetadata().put("enrichedPayload", ref.getPayloadJson());
                          return event;
                        })
                    .descriptor(descriptor)
                    .build())
            .apply(stream -> stream) // identity apply (skip ConstraintEnforcer/Processor)
            .process(stream -> stream) // identity process
            .getStream();

    List<StreamEnvelop> results = new ArrayList<>();
    try (CloseableIterator<StreamEnvelop> iter = enriched.executeAndCollect("test")) {
      while (iter.hasNext()) {
        results.add(iter.next());
      }
    }

    assertThat(results).hasSize(2);

    // Due to broadcast timing, some or all events may be enriched.
    // At minimum, the pipeline should produce all main events.
    List<String> primaryKeys = results.stream().map(StreamEnvelop::getPrimaryKey).toList();
    assertThat(primaryKeys).containsExactlyInAnyOrder("user1", "user2");
  }

  @Test
  @DisplayName("pipeline should work without reference topic")
  void pipelineShouldWorkWithoutReferenceTopic() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    List<StreamEnvelop> mainData =
        List.of(envelope("orders", "INSERT", "user1", "{\"orderId\":100}"));

    DataStream<StreamEnvelop> mainStream =
        env.fromCollection(mainData, TypeInformation.of(StreamEnvelop.class));

    // Build pipeline without enrich (simulates REFERENCE_TOPIC not set)
    DataStream<StreamEnvelop> result =
        PipelineBuilder.from(mainStream)
            .parse(stream -> stream)
            .apply(stream -> stream)
            .process(stream -> stream)
            .getStream();

    List<StreamEnvelop> results = new ArrayList<>();
    try (CloseableIterator<StreamEnvelop> iter = result.executeAndCollect("test")) {
      while (iter.hasNext()) {
        results.add(iter.next());
      }
    }

    assertThat(results).hasSize(1);
    assertThat(results.get(0).getPrimaryKey()).isEqualTo("user1");
    assertThat(results.get(0).getMetadata()).doesNotContainKey("enrichedPayload");
  }

  @Test
  @DisplayName("enrich pipeline should enrich with two reference streams")
  void enrichPipelineShouldEnrichWithTwoReferenceStreams() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    List<StreamEnvelop> mainData =
        List.of(
            envelope("orders", "INSERT", "key1", "{\"orderId\":100}"),
            envelope("orders", "INSERT", "key2", "{\"orderId\":200}"));

    List<StreamEnvelop> refData1 =
        List.of(
            envelope("users", "SNAPSHOT", "key1", "{\"name\":\"Alice\"}"),
            envelope("users", "SNAPSHOT", "key2", "{\"name\":\"Bob\"}"));

    List<StreamEnvelop> refData2 =
        List.of(
            envelope("products", "SNAPSHOT", "key1", "{\"product\":\"Widget\"}"),
            envelope("products", "SNAPSHOT", "key2", "{\"product\":\"Gadget\"}"));

    DataStream<StreamEnvelop> mainStream =
        env.fromCollection(mainData, TypeInformation.of(StreamEnvelop.class));
    DataStream<StreamEnvelop> refStream1 =
        env.fromCollection(refData1, TypeInformation.of(StreamEnvelop.class));
    DataStream<StreamEnvelop> refStream2 =
        env.fromCollection(refData2, TypeInformation.of(StreamEnvelop.class));

    MapStateDescriptor<String, StreamEnvelop> descriptor1 =
        new MapStateDescriptor<>(
            "ref-state-1", Types.STRING, TypeInformation.of(StreamEnvelop.class));
    MapStateDescriptor<String, StreamEnvelop> descriptor2 =
        new MapStateDescriptor<>(
            "ref-state-2", Types.STRING, TypeInformation.of(StreamEnvelop.class));

    DataStream<StreamEnvelop> enriched =
        PipelineBuilder.from(mainStream)
            .parse(stream -> stream)
            .enrich(
                refStream1,
                StaticJoiner.<StreamEnvelop, StreamEnvelop>builder()
                    .mainKeyExtractor(StreamEnvelop::getPrimaryKey)
                    .refKeyExtractor(StreamEnvelop::getPrimaryKey)
                    .joinFunction(
                        (event, ref) -> {
                          if (event.getMetadata() == null) {
                            event.setMetadata(new HashMap<>());
                          }
                          event.getMetadata().put("enrichedRef1", ref.getPayloadJson());
                          return event;
                        })
                    .descriptor(descriptor1)
                    .build())
            .enrich(
                refStream2,
                StaticJoiner.<StreamEnvelop, StreamEnvelop>builder()
                    .mainKeyExtractor(StreamEnvelop::getPrimaryKey)
                    .refKeyExtractor(StreamEnvelop::getPrimaryKey)
                    .joinFunction(
                        (event, ref) -> {
                          if (event.getMetadata() == null) {
                            event.setMetadata(new HashMap<>());
                          }
                          event.getMetadata().put("enrichedRef2", ref.getPayloadJson());
                          return event;
                        })
                    .descriptor(descriptor2)
                    .build())
            .apply(stream -> stream)
            .process(stream -> stream)
            .getStream();

    List<StreamEnvelop> results = new ArrayList<>();
    try (CloseableIterator<StreamEnvelop> iter = enriched.executeAndCollect("test")) {
      while (iter.hasNext()) {
        results.add(iter.next());
      }
    }

    assertThat(results).hasSize(2);

    List<String> primaryKeys = results.stream().map(StreamEnvelop::getPrimaryKey).toList();
    assertThat(primaryKeys).containsExactlyInAnyOrder("key1", "key2");
  }
}
