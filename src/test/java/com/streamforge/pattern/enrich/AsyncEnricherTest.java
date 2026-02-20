package com.streamforge.pattern.enrich;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class AsyncEnricherTest {

  private static List<String> execute(DataStream<String> stream) throws Exception {
    List<String> results = new ArrayList<>();
    try (CloseableIterator<String> iter = stream.executeAndCollect("test")) {
      while (iter.hasNext()) {
        results.add(iter.next());
      }
    }
    return results;
  }

  @Nested
  @DisplayName("Async enrichment")
  class AsyncEnrichment {

    @Test
    @DisplayName("should enrich events via async lookup")
    void shouldEnrichViaAsyncLookup() throws Exception {
      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      env.setParallelism(1);

      DataStream<String> source = env.fromCollection(List.of("user1", "user2"), Types.STRING);

      AsyncEnricher<String> enricher =
          AsyncEnricher.<String>builder()
              .lookupFunction(input -> CompletableFuture.completedFuture(input + "+enriched"))
              .timeout(Duration.ofSeconds(5))
              .build();

      DataStream<String> enriched = enricher.apply(source);

      List<String> results = execute(enriched);
      assertThat(results).containsExactlyInAnyOrder("user1+enriched", "user2+enriched");
    }

    @Test
    @DisplayName("should pass through original on lookup failure")
    void shouldPassThroughOnFailure() throws Exception {
      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      env.setParallelism(1);

      DataStream<String> source = env.fromCollection(List.of("user1"), Types.STRING);

      AsyncEnricher<String> enricher =
          AsyncEnricher.<String>builder()
              .lookupFunction(
                  input -> {
                    CompletableFuture<String> future = new CompletableFuture<>();
                    future.completeExceptionally(new RuntimeException("DB down"));
                    return future;
                  })
              .timeout(Duration.ofSeconds(5))
              .build();

      DataStream<String> enriched = enricher.apply(source);

      List<String> results = execute(enriched);
      assertThat(results).containsExactly("user1");
    }

    @Test
    @DisplayName("should pass through original on timeout")
    void shouldPassThroughOnTimeout() throws Exception {
      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      env.setParallelism(1);

      DataStream<String> source = env.fromCollection(List.of("user1"), Types.STRING);

      AsyncEnricher<String> enricher =
          AsyncEnricher.<String>builder()
              .lookupFunction(input -> new CompletableFuture<>())
              .timeout(Duration.ofMillis(100))
              .capacity(1)
              .build();

      DataStream<String> enriched = enricher.apply(source);

      List<String> results = execute(enriched);
      assertThat(results).containsExactly("user1");
    }

    @Test
    @DisplayName("should preserve ordering when ORDERED")
    void shouldPreserveOrdering() throws Exception {
      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      env.setParallelism(1);

      DataStream<String> source = env.fromCollection(List.of("a", "b", "c"), Types.STRING);

      AsyncEnricher<String> enricher =
          AsyncEnricher.<String>builder()
              .lookupFunction(input -> CompletableFuture.completedFuture(input + "+ok"))
              .ordering(AsyncEnricher.OrderingGuarantee.ORDERED)
              .timeout(Duration.ofSeconds(5))
              .build();

      DataStream<String> enriched = enricher.apply(source);

      List<String> results = execute(enriched);
      assertThat(results).containsExactly("a+ok", "b+ok", "c+ok");
    }

    @Test
    @DisplayName("should handle multiple concurrent lookups")
    void shouldHandleConcurrentLookups() throws Exception {
      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      env.setParallelism(1);

      DataStream<String> source =
          env.fromCollection(List.of("u1", "u2", "u3", "u4", "u5"), Types.STRING);

      AsyncEnricher<String> enricher =
          AsyncEnricher.<String>builder()
              .lookupFunction(input -> CompletableFuture.supplyAsync(() -> input + "+looked_up"))
              .timeout(Duration.ofSeconds(5))
              .capacity(10)
              .build();

      DataStream<String> enriched = enricher.apply(source);

      List<String> results = execute(enriched);
      assertThat(results).hasSize(5);
      assertThat(results).allMatch(r -> r.endsWith("+looked_up"));
    }
  }

  @Nested
  @DisplayName("Builder validation")
  class BuilderValidation {

    @Test
    @DisplayName("should throw when lookupFunction is null")
    void nullLookupFunctionThrows() {
      assertThatThrownBy(() -> AsyncEnricher.<String>builder().build())
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("lookupFunction must not be null");
    }

    @Test
    @DisplayName("should throw when capacity is zero")
    void zeroCapacityThrows() {
      assertThatThrownBy(
              () ->
                  AsyncEnricher.<String>builder()
                      .lookupFunction(input -> CompletableFuture.completedFuture(input))
                      .capacity(0)
                      .build())
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("capacity must be positive");
    }

    @Test
    @DisplayName("should use default values when not set")
    void shouldUseDefaults() {
      AsyncEnricher<String> enricher =
          AsyncEnricher.<String>builder()
              .lookupFunction(input -> CompletableFuture.completedFuture(input))
              .build();
      assertThat(enricher).isNotNull();
    }
  }
}
