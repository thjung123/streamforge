package com.streamforge.job.route;

import static org.assertj.core.api.Assertions.assertThat;

import com.streamforge.core.model.StreamEnvelop;
import com.streamforge.pattern.split.ParallelSplitter;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class EventRouterJobTest {

  private static StreamEnvelop envelope(String source, String op, String primaryKey) {
    return StreamEnvelop.builder()
        .operation(op)
        .source(source)
        .payloadJson("{\"_id\":\"" + primaryKey + "\"}")
        .primaryKey(primaryKey)
        .eventTime(Instant.now())
        .processedTime(Instant.now())
        .metadata(new HashMap<>())
        .build();
  }

  private static List<StreamEnvelop> collect(DataStream<StreamEnvelop> stream) throws Exception {
    List<StreamEnvelop> results = new ArrayList<>();
    try (CloseableIterator<StreamEnvelop> iter = stream.executeAndCollect("test")) {
      while (iter.hasNext()) {
        results.add(iter.next());
      }
    }
    return results;
  }

  @Test
  @DisplayName("should route orders events to orders side output")
  void shouldRouteOrdersToSideOutput() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    List<StreamEnvelop> data =
        List.of(
            envelope("orders", "INSERT", "o1"),
            envelope("orders", "UPDATE", "o2"),
            envelope("payments", "INSERT", "p1"));

    DataStream<StreamEnvelop> input =
        env.fromCollection(data, TypeInformation.of(StreamEnvelop.class));

    var splitter =
        ParallelSplitter.builder(TypeInformation.of(StreamEnvelop.class))
            .route("orders", e -> "orders".equals(e.getSource()))
            .route("payments", e -> "payments".equals(e.getSource()))
            .build();

    splitter.apply(input);
    DataStream<StreamEnvelop> orders = splitter.getSideOutput("orders");

    List<StreamEnvelop> results = collect(orders);

    assertThat(results).hasSize(2);
    assertThat(results).allMatch(e -> "orders".equals(e.getSource()));
  }

  @Test
  @DisplayName("should route payments events to payments side output")
  void shouldRoutePaymentsToSideOutput() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    List<StreamEnvelop> data =
        List.of(
            envelope("orders", "INSERT", "o1"),
            envelope("payments", "INSERT", "p1"),
            envelope("payments", "UPDATE", "p2"));

    DataStream<StreamEnvelop> input =
        env.fromCollection(data, TypeInformation.of(StreamEnvelop.class));

    var splitter =
        ParallelSplitter.builder(TypeInformation.of(StreamEnvelop.class))
            .route("orders", e -> "orders".equals(e.getSource()))
            .route("payments", e -> "payments".equals(e.getSource()))
            .build();

    splitter.apply(input);
    DataStream<StreamEnvelop> payments = splitter.getSideOutput("payments");

    List<StreamEnvelop> results = collect(payments);

    assertThat(results).hasSize(2);
    assertThat(results).allMatch(e -> "payments".equals(e.getSource()));
  }

  @Test
  @DisplayName("should send unmatched events to main output")
  void shouldSendUnmatchedToMainOutput() throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    List<StreamEnvelop> data =
        List.of(
            envelope("orders", "INSERT", "o1"),
            envelope("payments", "INSERT", "p1"),
            envelope("inventory", "INSERT", "i1"),
            envelope("shipping", "UPDATE", "s1"));

    DataStream<StreamEnvelop> input =
        env.fromCollection(data, TypeInformation.of(StreamEnvelop.class));

    var splitter =
        ParallelSplitter.builder(TypeInformation.of(StreamEnvelop.class))
            .route("orders", e -> "orders".equals(e.getSource()))
            .route("payments", e -> "payments".equals(e.getSource()))
            .build();

    DataStream<StreamEnvelop> main = splitter.apply(input);

    List<StreamEnvelop> results = collect(main);

    // In default (exclusive) mode, unmatched events go to main output
    assertThat(results).hasSize(2);
    List<String> sources = results.stream().map(StreamEnvelop::getSource).toList();
    assertThat(sources).containsExactlyInAnyOrder("inventory", "shipping");
  }
}
