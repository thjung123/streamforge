package com.streamforge.pattern.split;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import com.streamforge.pattern.observability.TimestampExtractor;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class OrderedFanInTest {

  private static final TimestampExtractor<String> EXTRACTOR = s -> Instant.now();

  @SuppressWarnings("unchecked")
  private static DataStream<String> mockStream() {
    return mock(DataStream.class);
  }

  private OneInputStreamOperatorTestHarness<String, String> createHarness(
      OrderedFanIn.MergeFunction<String> function) throws Exception {
    var operator = new ProcessOperator<>(function);
    var harness = new OneInputStreamOperatorTestHarness<>(operator);
    harness.open();
    return harness;
  }

  @Test
  @DisplayName("should pass through all merged elements")
  void mergedElementsPassThrough() throws Exception {
    var function = new OrderedFanIn.MergeFunction<String>();

    try (var harness = createHarness(function)) {
      harness.processElement(new StreamRecord<>("event1"));
      harness.processElement(new StreamRecord<>("event2"));
      harness.processElement(new StreamRecord<>("event3"));

      List<String> output =
          harness.extractOutputStreamRecords().stream().map(r -> (String) r.getValue()).toList();
      assertThat(output).containsExactly("event1", "event2", "event3");
    }
  }

  @SuppressWarnings("ConstantConditions")
  @Test
  @DisplayName("should throw when extractor is null")
  void nullExtractorThrows() {
    TimestampExtractor<String> nullExtractor = null;
    assertThatThrownBy(() -> OrderedFanIn.builder(nullExtractor))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("must not be null");
  }

  @Test
  @DisplayName("should throw when fewer than two sources")
  void fewerThanTwoSourcesThrows() {
    assertThatThrownBy(
            () ->
                OrderedFanIn.builder(EXTRACTOR)
                    .source("only", mockStream())
                    .maxDrift(Duration.ofSeconds(1))
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("At least two sources");
  }

  @Test
  @DisplayName("should throw when duplicate source name")
  void duplicateSourceNameThrows() {
    var builder = OrderedFanIn.builder(EXTRACTOR).source("dup", mockStream());

    assertThatThrownBy(() -> builder.source("dup", mockStream()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Duplicate source name");
  }

  @Test
  @DisplayName("should throw when stream is null")
  void nullStreamThrows() {
    assertThatThrownBy(() -> OrderedFanIn.builder(EXTRACTOR).source("name", null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("must not be null");
  }

  @Test
  @DisplayName("should throw when maxDrift is not set")
  void maxDriftNotSetThrows() {
    assertThatThrownBy(
            () ->
                OrderedFanIn.builder(EXTRACTOR)
                    .source("a", mockStream())
                    .source("b", mockStream())
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("maxDrift must be set");
  }

  @Test
  @DisplayName("should throw when source name is blank")
  void blankSourceNameThrows() {
    assertThatThrownBy(() -> OrderedFanIn.builder(EXTRACTOR).source("", mockStream()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("must not be null or blank");
  }
}
