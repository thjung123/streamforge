package com.streamforge.pattern.session;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class SessionAnalyzerTest {

  private static <R> List<SessionResult<R>> execute(DataStream<SessionResult<R>> stream)
      throws Exception {
    List<SessionResult<R>> results = new ArrayList<>();
    try (CloseableIterator<SessionResult<R>> iter = stream.executeAndCollect("test")) {
      while (iter.hasNext()) {
        results.add(iter.next());
      }
    }
    return results;
  }

  private static SessionAnalyzer.KeyExtractor<String> keyExtractor() {
    return value -> value.split(":")[0];
  }

  private static SessionAnalyzer.TimestampExtractor<String> timestampExtractor() {
    return value -> Long.parseLong(value.split(":")[1]);
  }

  private static SessionAnalyzer.Aggregator<String, Integer> countAggregator() {
    return List::size;
  }

  private static SessionAnalyzer.Aggregator<String, String> concatAggregator() {
    return events -> events.stream().map(e -> e.split(":")[2]).collect(Collectors.joining(","));
  }

  @Nested
  @DisplayName("Session detection")
  class SessionDetection {

    @Test
    @DisplayName("should detect single session within gap")
    void singleSession() throws Exception {
      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      env.setParallelism(1);

      DataStream<String> source =
          env.fromCollection(
              List.of("user1:1000:click", "user1:2000:scroll", "user1:3000:click"), Types.STRING);

      SessionAnalyzer<String, Integer> analyzer =
          SessionAnalyzer.<String, Integer>builder()
              .keyExtractor(keyExtractor())
              .timestampExtractor(timestampExtractor())
              .aggregator(countAggregator())
              .gap(Duration.ofSeconds(5))
              .build();

      List<SessionResult<Integer>> results = execute(analyzer.apply(source));

      assertThat(results).hasSize(1);
      assertThat(results.get(0).key()).isEqualTo("user1");
      assertThat(results.get(0).eventCount()).isEqualTo(3);
      assertThat(results.get(0).result()).isEqualTo(3);
    }

    @Test
    @DisplayName("should detect multiple sessions separated by gap")
    void multipleSessions() throws Exception {
      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      env.setParallelism(1);

      DataStream<String> source =
          env.fromCollection(
              List.of("user1:1000:a", "user1:2000:b", "user1:10000:c", "user1:11000:d"),
              Types.STRING);

      SessionAnalyzer<String, Integer> analyzer =
          SessionAnalyzer.<String, Integer>builder()
              .keyExtractor(keyExtractor())
              .timestampExtractor(timestampExtractor())
              .aggregator(countAggregator())
              .gap(Duration.ofSeconds(5))
              .build();

      List<SessionResult<Integer>> results = execute(analyzer.apply(source));
      results.sort(Comparator.comparing(SessionResult::sessionStart));

      assertThat(results).hasSize(2);
      assertThat(results.get(0).eventCount()).isEqualTo(2);
      assertThat(results.get(1).eventCount()).isEqualTo(2);
    }

    @Test
    @DisplayName("should handle multiple keys independently")
    void multipleKeys() throws Exception {
      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      env.setParallelism(1);

      DataStream<String> source =
          env.fromCollection(
              List.of("user1:1000:a", "user2:1000:b", "user1:2000:c", "user2:2000:d"),
              Types.STRING);

      SessionAnalyzer<String, Integer> analyzer =
          SessionAnalyzer.<String, Integer>builder()
              .keyExtractor(keyExtractor())
              .timestampExtractor(timestampExtractor())
              .aggregator(countAggregator())
              .gap(Duration.ofSeconds(5))
              .build();

      List<SessionResult<Integer>> results = execute(analyzer.apply(source));

      assertThat(results).hasSize(2);
      assertThat(results).allMatch(r -> r.eventCount() == 2);
      assertThat(results.stream().map(SessionResult::key).collect(Collectors.toSet()))
          .containsExactlyInAnyOrder("user1", "user2");
    }

    @Test
    @DisplayName("should set correct session start, end, and duration")
    void sessionMetadata() throws Exception {
      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      env.setParallelism(1);

      DataStream<String> source =
          env.fromCollection(List.of("user1:1000:a", "user1:3000:b", "user1:5000:c"), Types.STRING);

      SessionAnalyzer<String, Integer> analyzer =
          SessionAnalyzer.<String, Integer>builder()
              .keyExtractor(keyExtractor())
              .timestampExtractor(timestampExtractor())
              .aggregator(countAggregator())
              .gap(Duration.ofSeconds(10))
              .build();

      List<SessionResult<Integer>> results = execute(analyzer.apply(source));

      assertThat(results).hasSize(1);
      SessionResult<Integer> session = results.get(0);
      assertThat(session.sessionStart().toEpochMilli()).isEqualTo(1000);
      assertThat(session.sessionEnd().toEpochMilli()).isEqualTo(15000);
      assertThat(session.duration()).isEqualTo(Duration.ofMillis(14000));
    }
  }

  @Nested
  @DisplayName("Aggregation")
  class Aggregation {

    @Test
    @DisplayName("should apply custom aggregator")
    void customAggregator() throws Exception {
      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      env.setParallelism(1);

      DataStream<String> source =
          env.fromCollection(
              List.of("user1:1000:click", "user1:2000:scroll", "user1:3000:buy"), Types.STRING);

      SessionAnalyzer<String, String> analyzer =
          SessionAnalyzer.<String, String>builder()
              .keyExtractor(keyExtractor())
              .timestampExtractor(timestampExtractor())
              .aggregator(concatAggregator())
              .gap(Duration.ofSeconds(5))
              .build();

      List<SessionResult<String>> results = execute(analyzer.apply(source));

      assertThat(results).hasSize(1);
      assertThat(results.get(0).result()).isEqualTo("click,scroll,buy");
    }

    @Test
    @DisplayName("should handle single event session")
    void singleEventSession() throws Exception {
      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      env.setParallelism(1);

      DataStream<String> source = env.fromCollection(List.of("user1:1000:only"), Types.STRING);

      SessionAnalyzer<String, Integer> analyzer =
          SessionAnalyzer.<String, Integer>builder()
              .keyExtractor(keyExtractor())
              .timestampExtractor(timestampExtractor())
              .aggregator(countAggregator())
              .gap(Duration.ofSeconds(5))
              .build();

      List<SessionResult<Integer>> results = execute(analyzer.apply(source));

      assertThat(results).hasSize(1);
      assertThat(results.get(0).eventCount()).isEqualTo(1);
      assertThat(results.get(0).result()).isEqualTo(1);
    }
  }

  @Nested
  @DisplayName("Builder validation")
  class BuilderValidation {

    @Test
    @DisplayName("should throw when keyExtractor is null")
    void nullKeyExtractor() {
      assertThatThrownBy(
              () ->
                  SessionAnalyzer.<String, Integer>builder()
                      .timestampExtractor(timestampExtractor())
                      .aggregator(countAggregator())
                      .gap(Duration.ofSeconds(5))
                      .build())
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("keyExtractor must not be null");
    }

    @Test
    @DisplayName("should throw when timestampExtractor is null")
    void nullTimestampExtractor() {
      assertThatThrownBy(
              () ->
                  SessionAnalyzer.<String, Integer>builder()
                      .keyExtractor(keyExtractor())
                      .aggregator(countAggregator())
                      .gap(Duration.ofSeconds(5))
                      .build())
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("timestampExtractor must not be null");
    }

    @Test
    @DisplayName("should throw when aggregator is null")
    void nullAggregator() {
      assertThatThrownBy(
              () ->
                  SessionAnalyzer.<String, Integer>builder()
                      .keyExtractor(keyExtractor())
                      .timestampExtractor(timestampExtractor())
                      .gap(Duration.ofSeconds(5))
                      .build())
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("aggregator must not be null");
    }

    @Test
    @DisplayName("should throw when gap is null")
    void nullGap() {
      assertThatThrownBy(
              () ->
                  SessionAnalyzer.<String, Integer>builder()
                      .keyExtractor(keyExtractor())
                      .timestampExtractor(timestampExtractor())
                      .aggregator(countAggregator())
                      .build())
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("gap must not be null");
    }

    @Test
    @DisplayName("should use default outOfOrderness when not set")
    void defaultOutOfOrderness() {
      SessionAnalyzer<String, Integer> analyzer =
          SessionAnalyzer.<String, Integer>builder()
              .keyExtractor(keyExtractor())
              .timestampExtractor(timestampExtractor())
              .aggregator(countAggregator())
              .gap(Duration.ofSeconds(5))
              .build();
      assertThat(analyzer).isNotNull();
    }

    @Test
    @DisplayName("should build with allowedLateness")
    void withAllowedLateness() throws Exception {
      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      env.setParallelism(1);

      DataStream<String> source =
          env.fromCollection(List.of("user1:1000:a", "user1:2000:b"), Types.STRING);

      SessionAnalyzer<String, Integer> analyzer =
          SessionAnalyzer.<String, Integer>builder()
              .keyExtractor(keyExtractor())
              .timestampExtractor(timestampExtractor())
              .aggregator(countAggregator())
              .gap(Duration.ofSeconds(5))
              .allowedLateness(Duration.ofSeconds(10))
              .build();

      List<SessionResult<Integer>> results = execute(analyzer.apply(source));
      assertThat(results).hasSize(1);
      assertThat(results.get(0).eventCount()).isEqualTo(2);
    }

    @Test
    @DisplayName("should build with outOfOrderness")
    void withOutOfOrderness() throws Exception {
      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      env.setParallelism(1);

      DataStream<String> source =
          env.fromCollection(List.of("user1:3000:a", "user1:1000:b", "user1:2000:c"), Types.STRING);

      SessionAnalyzer<String, Integer> analyzer =
          SessionAnalyzer.<String, Integer>builder()
              .keyExtractor(keyExtractor())
              .timestampExtractor(timestampExtractor())
              .aggregator(countAggregator())
              .gap(Duration.ofSeconds(5))
              .outOfOrderness(Duration.ofSeconds(5))
              .build();

      List<SessionResult<Integer>> results = execute(analyzer.apply(source));
      assertThat(results).hasSize(1);
      assertThat(results.get(0).eventCount()).isEqualTo(3);
    }
  }
}
