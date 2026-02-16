package com.streamforge.pattern.schema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import com.streamforge.core.dlq.DLQPublisher;
import com.streamforge.core.model.StreamEnvelop;
import com.streamforge.pattern.schema.SchemaVersion.FieldType;
import java.util.List;
import java.util.Map;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

class SchemaEnforcerTest {

  private static final SchemaVersion V1 =
      SchemaVersion.builder()
          .required("_id", FieldType.NUMBER)
          .required("name", FieldType.STRING)
          .build();

  private static final SchemaVersion V2 =
      SchemaVersion.builder()
          .required("_id", FieldType.NUMBER)
          .required("name", FieldType.STRING)
          .required("email", FieldType.STRING)
          .optional("age")
          .build();

  @Nested
  @DisplayName("SchemaVersion validation")
  class SchemaVersionTest {

    @Test
    @DisplayName("should pass when all required fields present with correct types")
    void passWhenValid() {
      List<String> violations = V1.validate(Map.of("_id", 1, "name", "Alice"));
      assertThat(violations).isEmpty();
    }

    @Test
    @DisplayName("should fail when required field is missing")
    void failWhenMissing() {
      List<String> violations = V1.validate(Map.of("_id", 1));
      assertThat(violations).hasSize(1);
      assertThat(violations.get(0)).contains("name");
    }

    @Test
    @DisplayName("should fail when field type is wrong")
    void failWhenWrongType() {
      List<String> violations = V1.validate(Map.of("_id", "not-a-number", "name", "Alice"));
      assertThat(violations).hasSize(1);
      assertThat(violations.get(0)).contains("_id");
    }

    @Test
    @DisplayName("should fail when payload is null")
    void failWhenNull() {
      List<String> violations = V1.validate(null);
      assertThat(violations).containsExactly("payload is null");
    }

    @Test
    @DisplayName("should report unknown fields in strict mode")
    void reportUnknownFields() {
      SchemaVersion strictV1 =
          SchemaVersion.builder()
              .required("_id", FieldType.NUMBER)
              .required("name", FieldType.STRING)
              .strict()
              .build();
      List<String> violations =
          strictV1.validate(Map.of("_id", 1, "name", "Alice", "extra", "val"));
      assertThat(violations).hasSize(1);
      assertThat(violations.get(0)).contains("unknown field");
    }

    @Test
    @DisplayName("should allow extra fields in non-strict mode")
    void allowExtraFields() {
      List<String> violations = V1.validate(Map.of("_id", 1, "name", "Alice", "extra", "val"));
      assertThat(violations).isEmpty();
    }

    @Test
    @DisplayName("should allow optional fields")
    void allowOptionalFields() {
      List<String> violations =
          V2.validate(Map.of("_id", 1, "name", "Alice", "email", "a@b.com", "age", 25));
      assertThat(violations).isEmpty();
    }
  }

  @Nested
  @DisplayName("SchemaEnforcer pipeline")
  class EnforcerPipelineTest {

    @Test
    @DisplayName("should pass valid records")
    void passValidRecords() throws Exception {
      DLQPublisher mockPublisher = mock(DLQPublisher.class);

      try (MockedStatic<DLQPublisher> dlqStatic = mockStatic(DLQPublisher.class)) {
        dlqStatic.when(DLQPublisher::getInstance).thenReturn(mockPublisher);

        var function =
            new SchemaEnforcer.SchemaValidationFunction<>(
                StreamEnvelop::getPayloadAsMap, List.of(V1), "test");
        var operator = new ProcessOperator<>(function);

        try (var harness = new OneInputStreamOperatorTestHarness<>(operator)) {
          harness.open();

          harness.processElement(
              new StreamRecord<>(
                  StreamEnvelop.of("INSERT", "test", Map.of("_id", 1, "name", "Alice")), 1L));

          List<StreamEnvelop> output =
              harness.extractOutputStreamRecords().stream()
                  .map(r -> (StreamEnvelop) r.getValue())
                  .toList();

          assertThat(output).hasSize(1);
          verify(mockPublisher, never()).publish(any());
        }
      }
    }

    @Test
    @DisplayName("should drop and DLQ records that violate schema")
    void dropInvalidRecords() throws Exception {
      DLQPublisher mockPublisher = mock(DLQPublisher.class);

      try (MockedStatic<DLQPublisher> dlqStatic = mockStatic(DLQPublisher.class)) {
        dlqStatic.when(DLQPublisher::getInstance).thenReturn(mockPublisher);

        var function =
            new SchemaEnforcer.SchemaValidationFunction<>(
                StreamEnvelop::getPayloadAsMap, List.of(V1), "test");
        var operator = new ProcessOperator<>(function);

        try (var harness = new OneInputStreamOperatorTestHarness<>(operator)) {
          harness.open();

          harness.processElement(
              new StreamRecord<>(
                  StreamEnvelop.of("INSERT", "test", Map.of("_id", 1, "name", "Alice")), 1L));
          harness.processElement(
              new StreamRecord<>(StreamEnvelop.of("INSERT", "test", Map.of("_id", 2)), 2L));

          List<StreamEnvelop> output =
              harness.extractOutputStreamRecords().stream()
                  .map(r -> (StreamEnvelop) r.getValue())
                  .toList();

          assertThat(output).hasSize(1);
          assertThat(output.get(0).getPayloadAsMap().get("name")).isEqualTo("Alice");
          verify(mockPublisher).publish(any());
        }
      }
    }

    @Test
    @DisplayName("should accept records matching any schema version (evolution)")
    void acceptOlderSchemaVersion() throws Exception {
      DLQPublisher mockPublisher = mock(DLQPublisher.class);

      try (MockedStatic<DLQPublisher> dlqStatic = mockStatic(DLQPublisher.class)) {
        dlqStatic.when(DLQPublisher::getInstance).thenReturn(mockPublisher);

        var function =
            new SchemaEnforcer.SchemaValidationFunction<>(
                StreamEnvelop::getPayloadAsMap, List.of(V1, V2), "test");
        var operator = new ProcessOperator<>(function);

        try (var harness = new OneInputStreamOperatorTestHarness<>(operator)) {
          harness.open();

          // v1 format (no email) — should pass against v1
          harness.processElement(
              new StreamRecord<>(
                  StreamEnvelop.of("INSERT", "test", Map.of("_id", 1, "name", "Alice")), 1L));

          // v2 format (with email) — should pass against v2
          harness.processElement(
              new StreamRecord<>(
                  StreamEnvelop.of(
                      "INSERT", "test", Map.of("_id", 2, "name", "Bob", "email", "bob@test.com")),
                  2L));

          List<StreamEnvelop> output =
              harness.extractOutputStreamRecords().stream()
                  .map(r -> (StreamEnvelop) r.getValue())
                  .toList();

          assertThat(output).hasSize(2);
          verify(mockPublisher, never()).publish(any());
        }
      }
    }
  }
}
