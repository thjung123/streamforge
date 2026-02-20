package com.streamforge.job.ingest;

import static org.assertj.core.api.Assertions.assertThat;

import com.streamforge.core.model.StreamEnvelop;
import com.streamforge.pattern.split.OrderedFanIn;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class MergedIngestJobTest {

  private static StreamEnvelop envelope(String source, String op, Instant eventTime) {
    return StreamEnvelop.builder()
        .operation(op)
        .source(source)
        .payloadJson("{\"id\":1}")
        .eventTime(eventTime)
        .processedTime(Instant.now())
        .metadata(new HashMap<>())
        .build();
  }

  @Test
  @DisplayName("merged stream contains all events from both sources")
  void mergedStreamContainsAllEvents() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    Instant base = Instant.parse("2024-01-01T00:00:00Z");

    List<StreamEnvelop> ordersData =
        List.of(
            envelope("orders", "INSERT", base), envelope("orders", "UPDATE", base.plusSeconds(1)));

    List<StreamEnvelop> paymentsData =
        List.of(
            envelope("payments", "INSERT", base.plusMillis(500)),
            envelope("payments", "INSERT", base.plusSeconds(2)));

    DataStream<StreamEnvelop> orders =
        env.fromCollection(ordersData, TypeInformation.of(StreamEnvelop.class));
    DataStream<StreamEnvelop> payments =
        env.fromCollection(paymentsData, TypeInformation.of(StreamEnvelop.class));

    DataStream<StreamEnvelop> merged =
        OrderedFanIn.<StreamEnvelop>builder(StreamEnvelop::getEventTime)
            .source("orders", orders)
            .source("payments", payments)
            .maxDrift(Duration.ofSeconds(5))
            .build()
            .merge();

    List<StreamEnvelop> results = new ArrayList<>();
    try (CloseableIterator<StreamEnvelop> iter = merged.executeAndCollect("test")) {
      while (iter.hasNext()) {
        results.add(iter.next());
      }
    }

    assertThat(results).hasSize(4);

    List<String> sources = results.stream().map(StreamEnvelop::getSource).toList();
    assertThat(sources).containsExactlyInAnyOrder("orders", "orders", "payments", "payments");
  }

  @Test
  @DisplayName("source tagging adds metadata with source name")
  void sourceTaggingAddsMetadata() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    Instant base = Instant.parse("2024-01-01T00:00:00Z");

    DataStream<StreamEnvelop> orders =
        env.fromCollection(
            List.of(envelope("orders", "INSERT", base)), TypeInformation.of(StreamEnvelop.class));
    DataStream<StreamEnvelop> payments =
        env.fromCollection(
            List.of(envelope("payments", "INSERT", base.plusSeconds(1))),
            TypeInformation.of(StreamEnvelop.class));

    DataStream<StreamEnvelop> merged =
        OrderedFanIn.<StreamEnvelop>builder(StreamEnvelop::getEventTime)
            .source("orders", orders)
            .source("payments", payments)
            .maxDrift(Duration.ofSeconds(5))
            .sourceTag(
                (sourceName, element) -> {
                  if (element.getMetadata() == null) {
                    element.setMetadata(new HashMap<>());
                  }
                  element.getMetadata().put("ingestSource", sourceName);
                  return element;
                })
            .build()
            .merge();

    List<StreamEnvelop> results = new ArrayList<>();
    try (CloseableIterator<StreamEnvelop> iter = merged.executeAndCollect("test")) {
      while (iter.hasNext()) {
        results.add(iter.next());
      }
    }

    assertThat(results).hasSize(2);

    Map<String, String> tagValues = new HashMap<>();
    for (StreamEnvelop e : results) {
      assertThat(e.getMetadata()).containsKey("ingestSource");
      tagValues.put(e.getSource(), e.getMetadata().get("ingestSource"));
    }

    assertThat(tagValues).containsEntry("orders", "orders");
    assertThat(tagValues).containsEntry("payments", "payments");
  }
}
