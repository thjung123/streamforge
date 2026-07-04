package com.streamforge.pattern.dedup;

import static org.assertj.core.api.Assertions.assertThat;

import com.streamforge.core.model.StreamEnvelop;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
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

  @Test
  @DisplayName("should dedup CDC envelopes by the real primaryKey:eventTime key")
  void deduplicatesByCompositeCdcKey() throws Exception {
    var operator =
        new KeyedProcessOperator<>(
            new Deduplicator.DeduplicationFunction<StreamEnvelop>(Duration.ofMinutes(10), "test"));
    KeySelector<StreamEnvelop, String> key = e -> e.getPrimaryKey() + ":" + e.getEventTime();

    Instant t1 = Instant.ofEpochMilli(1_000);
    Instant t2 = Instant.ofEpochMilli(2_000);

    try (var harness = new KeyedOneInputStreamOperatorTestHarness<>(operator, key, Types.STRING)) {
      harness.open();

      harness.processElement(new StreamRecord<>(envelope("k1", t1))); // pass
      harness.processElement(new StreamRecord<>(envelope("k1", t1))); // same key+time -> dropped
      harness.processElement(new StreamRecord<>(envelope("k1", t2))); // same key, new time -> pass
      harness.processElement(new StreamRecord<>(envelope("k2", t1))); // pass

      List<String> keys =
          harness.extractOutputStreamRecords().stream()
              .map(r -> (StreamEnvelop) r.getValue())
              .map(e -> e.getPrimaryKey() + ":" + e.getEventTime())
              .toList();

      assertThat(keys).containsExactly("k1:" + t1, "k1:" + t2, "k2:" + t1);
    }
  }

  private static StreamEnvelop envelope(String primaryKey, Instant eventTime) {
    return StreamEnvelop.builder()
        .operation("INSERT")
        .primaryKey(primaryKey)
        .eventTime(eventTime)
        .payloadJson("{\"_id\":\"" + primaryKey + "\"}")
        .build();
  }
}
