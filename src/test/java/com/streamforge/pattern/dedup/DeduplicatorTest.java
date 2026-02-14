package com.streamforge.pattern.dedup;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class DeduplicatorTest {

  @Test
  @DisplayName("should pass all records when no duplicates exist")
  void allPassWhenNoDuplicates() throws Exception {
    var operator =
        new KeyedProcessOperator<>(
            new Deduplicator.DeduplicationFunction<String>(Duration.ofMinutes(10), "test"));

    try (var harness =
        new KeyedOneInputStreamOperatorTestHarness<>(operator, v -> v, Types.STRING)) {
      harness.open();

      harness.processElement(new StreamRecord<>("a", 1L));
      harness.processElement(new StreamRecord<>("b", 2L));
      harness.processElement(new StreamRecord<>("c", 3L));

      List<String> output =
          harness.extractOutputStreamRecords().stream().map(r -> (String) r.getValue()).toList();

      assertThat(output).containsExactly("a", "b", "c");
    }
  }

  @Test
  @DisplayName("should drop duplicate records with same key")
  void dropDuplicates() throws Exception {
    var operator =
        new KeyedProcessOperator<>(
            new Deduplicator.DeduplicationFunction<String>(Duration.ofMinutes(10), "test"));

    try (var harness =
        new KeyedOneInputStreamOperatorTestHarness<>(operator, v -> v, Types.STRING)) {
      harness.open();

      harness.processElement(new StreamRecord<>("a", 1L));
      harness.processElement(new StreamRecord<>("b", 2L));
      harness.processElement(new StreamRecord<>("a", 3L));
      harness.processElement(new StreamRecord<>("c", 4L));
      harness.processElement(new StreamRecord<>("b", 5L));
      harness.processElement(new StreamRecord<>("a", 6L));

      List<String> output =
          harness.extractOutputStreamRecords().stream().map(r -> (String) r.getValue()).toList();

      assertThat(output).containsExactly("a", "b", "c");
    }
  }

  @Test
  @DisplayName("should deduplicate by extracted key, not full value")
  void deduplicateByKey() throws Exception {
    var operator =
        new KeyedProcessOperator<>(
            new Deduplicator.DeduplicationFunction<String>(Duration.ofMinutes(10), "test"));

    try (var harness =
        new KeyedOneInputStreamOperatorTestHarness<>(
            operator, v -> v.substring(0, 1), Types.STRING)) {
      harness.open();

      harness.processElement(new StreamRecord<>("a1", 1L));
      harness.processElement(new StreamRecord<>("b1", 2L));
      harness.processElement(new StreamRecord<>("a2", 3L));
      harness.processElement(new StreamRecord<>("c1", 4L));

      List<String> output =
          harness.extractOutputStreamRecords().stream().map(r -> (String) r.getValue()).toList();

      assertThat(output).hasSize(3).containsExactly("a1", "b1", "c1");
    }
  }
}
