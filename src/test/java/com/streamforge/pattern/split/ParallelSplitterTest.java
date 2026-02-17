package com.streamforge.pattern.split;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Objects;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.util.OutputTag;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ParallelSplitterTest {

  private static final OutputTag<String> TAG_A = new OutputTag<>("routeA", Types.STRING);
  private static final OutputTag<String> TAG_B = new OutputTag<>("routeB", Types.STRING);

  private OneInputStreamOperatorTestHarness<String, String> createHarness(
      ParallelSplitter.SplitFunction<String> function) throws Exception {
    var operator = new ProcessOperator<>(function);
    var harness = new OneInputStreamOperatorTestHarness<>(operator);
    harness.open();
    return harness;
  }

  @Test
  @DisplayName("should route matching events to side output")
  void matchingEventsToSideOutput() throws Exception {
    var function =
        new ParallelSplitter.SplitFunction<>(
            List.of(new Route<>("routeA", s -> s.startsWith("A"), TAG_A)), false, "test");

    try (var harness = createHarness(function)) {
      harness.processElement(new StreamRecord<>("A-event1"));
      harness.processElement(new StreamRecord<>("A-event2"));

      var sideOutput = harness.getSideOutput(TAG_A);
      assertThat(sideOutput).hasSize(2);
      assertThat(sideOutput.poll().getValue()).isEqualTo("A-event1");
      assertThat(Objects.requireNonNull(sideOutput.poll()).getValue()).isEqualTo("A-event2");

      var mainOutput =
          harness.extractOutputStreamRecords().stream().map(r -> (String) r.getValue()).toList();
      assertThat(mainOutput).isEmpty();
    }
  }

  @Test
  @DisplayName("should send unmatched events to main output")
  void unmatchedEventsToMainOutput() throws Exception {
    var function =
        new ParallelSplitter.SplitFunction<>(
            List.of(new Route<>("routeA", s -> s.startsWith("A"), TAG_A)), false, "test");

    try (var harness = createHarness(function)) {
      harness.processElement(new StreamRecord<>("B-event1"));
      harness.processElement(new StreamRecord<>("C-event2"));

      var sideOutput = harness.getSideOutput(TAG_A);
      assertThat(sideOutput).isNullOrEmpty();

      var mainOutput =
          harness.extractOutputStreamRecords().stream().map(r -> (String) r.getValue()).toList();
      assertThat(mainOutput).containsExactly("B-event1", "C-event2");
    }
  }

  @Test
  @DisplayName("should use first-match-wins in exclusive mode")
  void firstMatchWinsInExclusiveMode() throws Exception {
    var function =
        new ParallelSplitter.SplitFunction<>(
            List.of(
                new Route<>("routeA", s -> s.length() > 2, TAG_A),
                new Route<>("routeB", s -> s.startsWith("A"), TAG_B)),
            false,
            "test");

    try (var harness = createHarness(function)) {
      // "ABC" matches both routeA (length > 2) and routeB (starts with A)
      harness.processElement(new StreamRecord<>("ABC"));

      var sideA = harness.getSideOutput(TAG_A);
      var sideB = harness.getSideOutput(TAG_B);

      assertThat(sideA).hasSize(1);
      assertThat(sideA.poll().getValue()).isEqualTo("ABC");
      assertThat(sideB).isNullOrEmpty();
    }
  }

  @Test
  @DisplayName("should send all events to main and matching to side output in copyToMain mode")
  void copyToMainMode() throws Exception {
    var function =
        new ParallelSplitter.SplitFunction<>(
            List.of(new Route<>("routeA", s -> s.startsWith("A"), TAG_A)), true, "test");

    try (var harness = createHarness(function)) {
      harness.processElement(new StreamRecord<>("A-event"));
      harness.processElement(new StreamRecord<>("B-event"));

      var sideA = harness.getSideOutput(TAG_A);
      assertThat(sideA).hasSize(1);
      assertThat(sideA.poll().getValue()).isEqualTo("A-event");

      var mainOutput =
          harness.extractOutputStreamRecords().stream().map(r -> (String) r.getValue()).toList();
      assertThat(mainOutput).containsExactly("A-event", "B-event");
    }
  }

  @Test
  @DisplayName("should route to multiple side outputs in copyToMain mode")
  void multiRouteCopyToMainMode() throws Exception {
    var function =
        new ParallelSplitter.SplitFunction<>(
            List.of(
                new Route<>("routeA", s -> s.contains("X"), TAG_A),
                new Route<>("routeB", s -> s.contains("Y"), TAG_B)),
            true,
            "test");

    try (var harness = createHarness(function)) {
      // "XY" matches both routes
      harness.processElement(new StreamRecord<>("XY"));

      var sideA = harness.getSideOutput(TAG_A);
      var sideB = harness.getSideOutput(TAG_B);

      assertThat(sideA).hasSize(1);
      assertThat(sideA.poll().getValue()).isEqualTo("XY");
      assertThat(sideB).hasSize(1);
      assertThat(sideB.poll().getValue()).isEqualTo("XY");

      var mainOutput =
          harness.extractOutputStreamRecords().stream().map(r -> (String) r.getValue()).toList();
      assertThat(mainOutput).containsExactly("XY");
    }
  }

  @Test
  @DisplayName("should throw when building with empty routes")
  void emptyRoutesBuildThrows() {
    assertThatThrownBy(() -> ParallelSplitter.builder(Types.STRING).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("At least one route");
  }

  @Test
  @DisplayName("should throw when accessing unknown route name")
  void unknownRouteNameThrows() {
    var splitter = ParallelSplitter.builder(Types.STRING).route("routeA", s -> true).build();

    assertThatThrownBy(() -> splitter.tag("nonexistent"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unknown route");
  }
}
