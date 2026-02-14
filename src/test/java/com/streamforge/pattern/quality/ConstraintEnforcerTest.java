package com.streamforge.pattern.quality;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import com.streamforge.core.dlq.DLQPublisher;
import com.streamforge.core.model.StreamEnvelop;
import com.streamforge.pattern.quality.rules.FormatRule;
import com.streamforge.pattern.quality.rules.NotNullRule;
import com.streamforge.pattern.quality.rules.RangeRule;
import java.util.List;
import java.util.Map;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

class ConstraintEnforcerTest {

  @Nested
  @DisplayName("NotNullRule")
  class NotNullRuleTest {

    private final NotNullRule rule = new NotNullRule("name");

    @Test
    @DisplayName("should pass when field exists and is not null")
    void passWhenFieldExists() {
      StreamEnvelop envelop = StreamEnvelop.of("INSERT", "test", Map.of("name", "John"));
      assertThat(rule.validate(envelop)).isNull();
    }

    @Test
    @DisplayName("should fail when field is missing")
    void failWhenFieldMissing() {
      StreamEnvelop envelop = StreamEnvelop.of("INSERT", "test", Map.of("age", 25));
      assertThat(rule.validate(envelop)).contains("name");
    }

    @Test
    @DisplayName("should fail when payload is null")
    void failWhenPayloadNull() {
      StreamEnvelop envelop = StreamEnvelop.of("INSERT", "test", null);
      assertThat(rule.validate(envelop)).contains("name");
    }
  }

  @Nested
  @DisplayName("RangeRule")
  class RangeRuleTest {

    private final RangeRule rule = new RangeRule("age", 0, 200);

    @Test
    @DisplayName("should pass when value is within range")
    void passWhenInRange() {
      StreamEnvelop envelop = StreamEnvelop.of("INSERT", "test", Map.of("age", 25));
      assertThat(rule.validate(envelop)).isNull();
    }

    @Test
    @DisplayName("should fail when value is below minimum")
    void failWhenBelowMin() {
      StreamEnvelop envelop = StreamEnvelop.of("INSERT", "test", Map.of("age", -1));
      assertThat(rule.validate(envelop)).contains("age");
    }

    @Test
    @DisplayName("should fail when value exceeds maximum")
    void failWhenAboveMax() {
      StreamEnvelop envelop = StreamEnvelop.of("INSERT", "test", Map.of("age", 300));
      assertThat(rule.validate(envelop)).contains("age");
    }

    @Test
    @DisplayName("should skip when field is absent")
    void skipWhenFieldAbsent() {
      StreamEnvelop envelop = StreamEnvelop.of("INSERT", "test", Map.of("name", "John"));
      assertThat(rule.validate(envelop)).isNull();
    }

    @Test
    @DisplayName("should fail when field is not a number")
    void failWhenNotNumber() {
      StreamEnvelop envelop = StreamEnvelop.of("INSERT", "test", Map.of("age", "twenty"));
      assertThat(rule.validate(envelop)).contains("not a number");
    }
  }

  @Nested
  @DisplayName("FormatRule")
  class FormatRuleTest {

    @Test
    @DisplayName("should pass for valid email")
    void passValidEmail() {
      FormatRule rule = FormatRule.email("email");
      StreamEnvelop envelop = StreamEnvelop.of("INSERT", "test", Map.of("email", "a@b.com"));
      assertThat(rule.validate(envelop)).isNull();
    }

    @Test
    @DisplayName("should fail for invalid email")
    void failInvalidEmail() {
      FormatRule rule = FormatRule.email("email");
      StreamEnvelop envelop = StreamEnvelop.of("INSERT", "test", Map.of("email", "not-email"));
      assertThat(rule.validate(envelop)).contains("email");
    }

    @Test
    @DisplayName("should pass for valid date")
    void passValidDate() {
      FormatRule rule = FormatRule.date("created");
      StreamEnvelop envelop = StreamEnvelop.of("INSERT", "test", Map.of("created", "2026-02-14"));
      assertThat(rule.validate(envelop)).isNull();
    }

    @Test
    @DisplayName("should fail for invalid date")
    void failInvalidDate() {
      FormatRule rule = FormatRule.date("created");
      StreamEnvelop envelop = StreamEnvelop.of("INSERT", "test", Map.of("created", "14/02/2026"));
      assertThat(rule.validate(envelop)).contains("created");
    }

    @Test
    @DisplayName("should skip when field is absent")
    void skipWhenFieldAbsent() {
      FormatRule rule = FormatRule.email("email");
      StreamEnvelop envelop = StreamEnvelop.of("INSERT", "test", Map.of("name", "John"));
      assertThat(rule.validate(envelop)).isNull();
    }
  }

  @Nested
  @DisplayName("ConstraintEnforcer pipeline")
  class EnforcerPipelineTest {

    @Test
    @DisplayName("should pass all records when all constraints are satisfied")
    void allPassWhenValid() throws Exception {
      DLQPublisher mockPublisher = mock(DLQPublisher.class);

      try (MockedStatic<DLQPublisher> dlqStatic = mockStatic(DLQPublisher.class)) {
        dlqStatic.when(DLQPublisher::getInstance).thenReturn(mockPublisher);

        var function =
            new ConstraintEnforcer.ValidationFunction<>(
                List.of(new NotNullRule("name"), new RangeRule("age", 0, 200)), "test");
        var operator = new ProcessOperator<>(function);

        try (var harness = new OneInputStreamOperatorTestHarness<>(operator)) {
          harness.open();

          harness.processElement(
              new StreamRecord<>(
                  StreamEnvelop.of("INSERT", "test", Map.of("name", "Alice", "age", 30)), 1L));
          harness.processElement(
              new StreamRecord<>(
                  StreamEnvelop.of("INSERT", "test", Map.of("name", "Bob", "age", 25)), 2L));

          List<StreamEnvelop> output =
              harness.extractOutputStreamRecords().stream()
                  .map(r -> (StreamEnvelop) r.getValue())
                  .toList();

          assertThat(output).hasSize(2);
        }
      }
    }

    @Test
    @DisplayName("should drop records that violate constraints")
    void dropInvalidRecords() throws Exception {
      DLQPublisher mockPublisher = mock(DLQPublisher.class);

      try (MockedStatic<DLQPublisher> dlqStatic = mockStatic(DLQPublisher.class)) {
        dlqStatic.when(DLQPublisher::getInstance).thenReturn(mockPublisher);

        var function =
            new ConstraintEnforcer.ValidationFunction<>(
                List.of(new NotNullRule("name"), new RangeRule("age", 0, 200)), "test");
        var operator = new ProcessOperator<>(function);

        try (var harness = new OneInputStreamOperatorTestHarness<>(operator)) {
          harness.open();

          harness.processElement(
              new StreamRecord<>(
                  StreamEnvelop.of("INSERT", "test", Map.of("name", "Alice", "age", 30)), 1L));
          harness.processElement(
              new StreamRecord<>(StreamEnvelop.of("INSERT", "test", Map.of("age", 25)), 2L));
          harness.processElement(
              new StreamRecord<>(
                  StreamEnvelop.of("INSERT", "test", Map.of("name", "Charlie", "age", -5)), 3L));

          List<StreamEnvelop> output =
              harness.extractOutputStreamRecords().stream()
                  .map(r -> (StreamEnvelop) r.getValue())
                  .toList();

          assertThat(output).hasSize(1);
          assertThat(output.get(0).getPayloadAsMap().get("name")).isEqualTo("Alice");
        }
      }
    }
  }
}
