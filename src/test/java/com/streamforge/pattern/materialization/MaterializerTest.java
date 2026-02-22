package com.streamforge.pattern.materialization;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class MaterializerTest {

  private static final Duration TTL = Duration.ofMinutes(10);

  @Nested
  @DisplayName("INSERT")
  class Insert {

    @Test
    @DisplayName("should emit INSERT for new key")
    void insertForNewKey() throws Exception {
      var operator =
          new KeyedProcessOperator<>(
              new Materializer.MaterializeFunction<String>(TTL, (a, b) -> b, null));

      try (var harness =
          new KeyedOneInputStreamOperatorTestHarness<>(operator, v -> v, Types.STRING)) {
        harness.open();

        harness.processElement(new StreamRecord<>("hello", 100L));

        List<ChangelogEvent<String>> output = extractOutput(harness);

        assertThat(output).hasSize(1);
        assertThat(output.get(0).type()).isEqualTo(ChangelogEvent.ChangeType.INSERT);
        assertThat(output.get(0).key()).isEqualTo("hello");
        assertThat(output.get(0).before()).isNull();
        assertThat(output.get(0).after()).isEqualTo("hello");
      }
    }

    @Test
    @DisplayName("should emit INSERT for each distinct key")
    void insertForDistinctKeys() throws Exception {
      var operator =
          new KeyedProcessOperator<>(
              new Materializer.MaterializeFunction<String>(TTL, (a, b) -> b, null));

      try (var harness =
          new KeyedOneInputStreamOperatorTestHarness<>(operator, v -> v, Types.STRING)) {
        harness.open();

        harness.processElement(new StreamRecord<>("a", 1L));
        harness.processElement(new StreamRecord<>("b", 2L));
        harness.processElement(new StreamRecord<>("c", 3L));

        List<ChangelogEvent<String>> output = extractOutput(harness);

        assertThat(output).hasSize(3);
        assertThat(output).allMatch(e -> e.type() == ChangelogEvent.ChangeType.INSERT);
      }
    }
  }

  @Nested
  @DisplayName("UPDATE")
  class Update {

    @Test
    @DisplayName("should emit UPDATE with before/after for existing key")
    void updateForExistingKey() throws Exception {
      var operator =
          new KeyedProcessOperator<>(
              new Materializer.MaterializeFunction<String>(TTL, (a, b) -> b, null));

      try (var harness =
          new KeyedOneInputStreamOperatorTestHarness<>(
              operator, v -> v.substring(0, 1), Types.STRING)) {
        harness.open();

        harness.processElement(new StreamRecord<>("v1", 1L));
        harness.processElement(new StreamRecord<>("v2", 2L));

        List<ChangelogEvent<String>> output = extractOutput(harness);

        assertThat(output).hasSize(2);

        ChangelogEvent<String> update = output.get(1);
        assertThat(update.type()).isEqualTo(ChangelogEvent.ChangeType.UPDATE);
        assertThat(update.before()).isEqualTo("v1");
        assertThat(update.after()).isEqualTo("v2");
      }
    }

    @Test
    @DisplayName("should apply custom merge function on update")
    void customMergeFunction() throws Exception {
      Materializer.MergeFunction<String> concat = (existing, incoming) -> existing + "+" + incoming;

      var operator =
          new KeyedProcessOperator<>(new Materializer.MaterializeFunction<>(TTL, concat, null));

      try (var harness =
          new KeyedOneInputStreamOperatorTestHarness<>(
              operator, v -> v.substring(0, 1), Types.STRING)) {
        harness.open();

        harness.processElement(new StreamRecord<>("a1", 1L));
        harness.processElement(new StreamRecord<>("a2", 2L));

        List<ChangelogEvent<String>> output = extractOutput(harness);

        assertThat(output).hasSize(2);

        ChangelogEvent<String> update = output.get(1);
        assertThat(update.type()).isEqualTo(ChangelogEvent.ChangeType.UPDATE);
        assertThat(update.before()).isEqualTo("a1");
        assertThat(update.after()).isEqualTo("a1+a2");
      }
    }
  }

  @Nested
  @DisplayName("DELETE")
  class Delete {

    @Test
    @DisplayName("should emit DELETE when predicate matches")
    void deleteWhenPredicateMatches() throws Exception {
      Materializer.DeletePredicate<String> isDelete = v -> v.startsWith("DEL:");

      var operator =
          new KeyedProcessOperator<>(
              new Materializer.MaterializeFunction<>(TTL, (a, b) -> b, isDelete));

      try (var harness =
          new KeyedOneInputStreamOperatorTestHarness<>(
              operator, v -> v.startsWith("DEL:") ? v.substring(4) : v, Types.STRING)) {
        harness.open();

        harness.processElement(new StreamRecord<>("hello", 1L));
        harness.processElement(new StreamRecord<>("DEL:hello", 2L));

        List<ChangelogEvent<String>> output = extractOutput(harness);

        assertThat(output).hasSize(2);

        ChangelogEvent<String> insert = output.get(0);
        assertThat(insert.type()).isEqualTo(ChangelogEvent.ChangeType.INSERT);

        ChangelogEvent<String> delete = output.get(1);
        assertThat(delete.type()).isEqualTo(ChangelogEvent.ChangeType.DELETE);
        assertThat(delete.before()).isEqualTo("hello");
        assertThat(delete.after()).isNull();
      }
    }

    @Test
    @DisplayName("should re-INSERT after DELETE for same key")
    void reInsertAfterDelete() throws Exception {
      Materializer.DeletePredicate<String> isDelete = v -> v.startsWith("DEL:");

      var operator =
          new KeyedProcessOperator<>(
              new Materializer.MaterializeFunction<>(TTL, (a, b) -> b, isDelete));

      try (var harness =
          new KeyedOneInputStreamOperatorTestHarness<>(
              operator, v -> v.startsWith("DEL:") ? v.substring(4) : v, Types.STRING)) {
        harness.open();

        harness.processElement(new StreamRecord<>("hello", 1L));
        harness.processElement(new StreamRecord<>("DEL:hello", 2L));
        harness.processElement(new StreamRecord<>("hello", 3L));

        List<ChangelogEvent<String>> output = extractOutput(harness);

        assertThat(output).hasSize(3);
        assertThat(output.get(0).type()).isEqualTo(ChangelogEvent.ChangeType.INSERT);
        assertThat(output.get(1).type()).isEqualTo(ChangelogEvent.ChangeType.DELETE);
        assertThat(output.get(2).type()).isEqualTo(ChangelogEvent.ChangeType.INSERT);
      }
    }
  }

  @Nested
  @DisplayName("Builder validation")
  class BuilderValidation {

    @Test
    @DisplayName("should throw when keyExtractor is null")
    void nullKeyExtractor() {
      assertThatThrownBy(() -> Materializer.builder().ttl(TTL).build())
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("keyExtractor");
    }

    @Test
    @DisplayName("should throw when ttl is null")
    void nullTtl() {
      assertThatThrownBy(() -> Materializer.<String>builder().keyExtractor(v -> v).build())
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("ttl");
    }

    @Test
    @DisplayName("should use REPLACE as default merge function")
    void defaultMergeFunction() {
      Materializer<String> materializer =
          Materializer.<String>builder().keyExtractor(v -> v).ttl(TTL).build();

      assertThat(materializer).isNotNull();
    }
  }

  @SuppressWarnings("unchecked")
  private static <T> List<ChangelogEvent<T>> extractOutput(
      KeyedOneInputStreamOperatorTestHarness<?, ?, ?> harness) {
    return harness.extractOutputStreamRecords().stream()
        .map(r -> (ChangelogEvent<T>) ((StreamRecord<?>) r).getValue())
        .toList();
  }
}
