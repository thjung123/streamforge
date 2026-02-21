package com.streamforge.job.session;

import static org.assertj.core.api.Assertions.assertThat;

import com.streamforge.core.model.StreamEnvelop;
import com.streamforge.pattern.session.SessionAnalyzer;
import com.streamforge.pattern.session.SessionResult;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class UserSessionAnalysisJobTest {

  private static StreamEnvelop event(String userId, String operation, long epochMilli) {
    return StreamEnvelop.builder()
        .operation(operation)
        .source("clickstream")
        .payloadJson("{}")
        .primaryKey(userId)
        .eventTime(Instant.ofEpochMilli(epochMilli))
        .processedTime(Instant.now())
        .metadata(new HashMap<>())
        .build();
  }

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

  @Nested
  @DisplayName("Session analysis pipeline")
  class SessionAnalysisPipeline {

    @Test
    @DisplayName("should detect single user session")
    void singleUserSession() throws Exception {
      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      env.setParallelism(1);

      List<StreamEnvelop> data =
          List.of(
              event("user-1", "CLICK", 1000),
              event("user-1", "SCROLL", 2000),
              event("user-1", "CLICK", 3000));

      DataStream<StreamEnvelop> source =
          env.fromCollection(data, TypeInformation.of(StreamEnvelop.class));

      SessionAnalyzer<StreamEnvelop, String> analyzer =
          UserSessionAnalysisJob.buildAnalyzer(Duration.ofSeconds(5));

      List<SessionResult<String>> results = execute(analyzer.apply(source));

      assertThat(results).hasSize(1);
      assertThat(results.get(0).key()).isEqualTo("user-1");
      assertThat(results.get(0).eventCount()).isEqualTo(3);
      assertThat(results.get(0).result()).contains("CLICK");
      assertThat(results.get(0).result()).contains("SCROLL");
    }

    @Test
    @DisplayName("should detect multiple sessions for same user")
    void multipleSessionsSameUser() throws Exception {
      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      env.setParallelism(1);

      List<StreamEnvelop> data =
          List.of(
              event("user-1", "CLICK", 1000),
              event("user-1", "SCROLL", 2000),
              event("user-1", "CLICK", 20000),
              event("user-1", "BUY", 21000));

      DataStream<StreamEnvelop> source =
          env.fromCollection(data, TypeInformation.of(StreamEnvelop.class));

      SessionAnalyzer<StreamEnvelop, String> analyzer =
          UserSessionAnalysisJob.buildAnalyzer(Duration.ofSeconds(5));

      List<SessionResult<String>> results = execute(analyzer.apply(source));
      results.sort(Comparator.comparing(SessionResult::sessionStart));

      assertThat(results).hasSize(2);
      assertThat(results.get(0).eventCount()).isEqualTo(2);
      assertThat(results.get(1).eventCount()).isEqualTo(2);
      assertThat(results.get(1).result()).contains("BUY");
    }

    @Test
    @DisplayName("should handle multiple users independently")
    void multipleUsers() throws Exception {
      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      env.setParallelism(1);

      List<StreamEnvelop> data =
          List.of(
              event("user-1", "CLICK", 1000),
              event("user-2", "SCROLL", 1500),
              event("user-1", "BUY", 2000),
              event("user-2", "CLICK", 2500));

      DataStream<StreamEnvelop> source =
          env.fromCollection(data, TypeInformation.of(StreamEnvelop.class));

      SessionAnalyzer<StreamEnvelop, String> analyzer =
          UserSessionAnalysisJob.buildAnalyzer(Duration.ofSeconds(5));

      List<SessionResult<String>> results = execute(analyzer.apply(source));

      assertThat(results).hasSize(2);
      assertThat(results.stream().map(SessionResult::key).collect(Collectors.toSet()))
          .containsExactlyInAnyOrder("user-1", "user-2");
      assertThat(results).allMatch(r -> r.eventCount() == 2);
    }

    @Test
    @DisplayName("should aggregate actions in session result")
    void aggregateActions() throws Exception {
      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      env.setParallelism(1);

      List<StreamEnvelop> data =
          List.of(
              event("user-1", "VIEW", 1000),
              event("user-1", "ADD_CART", 2000),
              event("user-1", "CHECKOUT", 3000));

      DataStream<StreamEnvelop> source =
          env.fromCollection(data, TypeInformation.of(StreamEnvelop.class));

      SessionAnalyzer<StreamEnvelop, String> analyzer =
          UserSessionAnalysisJob.buildAnalyzer(Duration.ofSeconds(5));

      List<SessionResult<String>> results = execute(analyzer.apply(source));

      assertThat(results).hasSize(1);
      String result = results.get(0).result();
      assertThat(result).contains("VIEW");
      assertThat(result).contains("ADD_CART");
      assertThat(result).contains("CHECKOUT");
      assertThat(result).contains("\"count\":3");
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
        if (job instanceof UserSessionAnalysisJob) {
          found = true;
          assertThat(job.name()).isEqualTo("UserSessionAnalysis");
          break;
        }
      }
      assertThat(found).isTrue();
    }
  }
}
