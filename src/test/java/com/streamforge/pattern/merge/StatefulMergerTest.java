package com.streamforge.pattern.merge;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class StatefulMergerTest {

  @Test
  @DisplayName("should forward first event for a key")
  void forwardFirstEvent() throws Exception {
    var operator = createOperator(Set.of());

    try (var harness = createHarness(operator)) {
      harness.open();

      harness.processElement(new StreamRecord<>(event("1", "Alice", "2026-01-01"), 1L));

      List<String> output = extractValues(harness);
      assertThat(output).hasSize(1);
    }
  }

  @Test
  @DisplayName("should forward when payload actually changes")
  void forwardOnActualChange() throws Exception {
    var operator = createOperator(Set.of());

    try (var harness = createHarness(operator)) {
      harness.open();

      harness.processElement(new StreamRecord<>(event("1", "Alice", "2026-01-01"), 1L));
      harness.processElement(new StreamRecord<>(event("1", "Alicia", "2026-01-02"), 2L));

      List<String> output = extractValues(harness);
      assertThat(output).hasSize(2);
    }
  }

  @Test
  @DisplayName("should suppress no-op update with identical payload")
  void suppressIdenticalPayload() throws Exception {
    var operator = createOperator(Set.of());

    try (var harness = createHarness(operator)) {
      harness.open();

      harness.processElement(new StreamRecord<>(event("1", "Alice", "2026-01-01"), 1L));
      harness.processElement(new StreamRecord<>(event("1", "Alice", "2026-01-01"), 2L));

      List<String> output = extractValues(harness);
      assertThat(output).hasSize(1);
    }
  }

  @Test
  @DisplayName("should suppress when only excluded fields differ")
  void suppressWhenOnlyExcludedFieldsDiffer() throws Exception {
    var operator = createOperator(Set.of("updatedAt"));

    try (var harness = createHarness(operator)) {
      harness.open();

      harness.processElement(new StreamRecord<>(event("1", "Alice", "2026-01-01"), 1L));
      harness.processElement(new StreamRecord<>(event("1", "Alice", "2026-01-02"), 2L));

      List<String> output = extractValues(harness);
      assertThat(output).hasSize(1);
    }
  }

  @Test
  @DisplayName("should forward when non-excluded fields change even if excluded fields also change")
  void forwardWhenNonExcludedFieldChanges() throws Exception {
    var operator = createOperator(Set.of("updatedAt"));

    try (var harness = createHarness(operator)) {
      harness.open();

      harness.processElement(new StreamRecord<>(event("1", "Alice", "2026-01-01"), 1L));
      harness.processElement(new StreamRecord<>(event("1", "Alicia", "2026-01-02"), 2L));

      List<String> output = extractValues(harness);
      assertThat(output).hasSize(2);
    }
  }

  @Test
  @DisplayName("should track state independently per key")
  void independentPerKey() throws Exception {
    var operator = createOperator(Set.of());

    try (var harness = createHarness(operator)) {
      harness.open();

      harness.processElement(new StreamRecord<>(event("1", "Alice", "2026-01-01"), 1L));
      harness.processElement(new StreamRecord<>(event("2", "Bob", "2026-01-01"), 2L));
      harness.processElement(new StreamRecord<>(event("1", "Alice", "2026-01-01"), 3L));
      harness.processElement(new StreamRecord<>(event("2", "Bobby", "2026-01-01"), 4L));

      List<String> output = extractValues(harness);
      assertThat(output)
          .hasSize(3)
          .containsExactly("1:Alice:2026-01-01", "2:Bob:2026-01-01", "2:Bobby:2026-01-01");
    }
  }

  private static String event(String id, String name, String updatedAt) {
    return id + ":" + name + ":" + updatedAt;
  }

  private static Map<String, Object> parsePayload(String value) {
    String[] parts = value.split(":");
    Map<String, Object> map = new HashMap<>();
    map.put("name", parts[1]);
    map.put("updatedAt", parts[2]);
    return map;
  }

  private static KeyedProcessOperator<String, String, String> createOperator(
      Set<String> excludedFields) {
    return new KeyedProcessOperator<>(
        new StatefulMerger.MergeFunction<>(
            StatefulMergerTest::parsePayload, excludedFields, Duration.ofHours(24), "test"));
  }

  private static KeyedOneInputStreamOperatorTestHarness<String, String, String> createHarness(
      KeyedProcessOperator<String, String, String> operator) throws Exception {
    return new KeyedOneInputStreamOperatorTestHarness<>(
        operator, v -> v.split(":")[0], Types.STRING);
  }

  private static List<String> extractValues(
      KeyedOneInputStreamOperatorTestHarness<String, String, String> harness) {
    return harness.extractOutputStreamRecords().stream().map(r -> (String) r.getValue()).toList();
  }
}
