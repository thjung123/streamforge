package com.streamforge.pattern.enrich;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.co.CoBroadcastWithNonKeyedOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.TwoInputStreamOperatorTestHarness;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class StaticJoinerTest {

  private static final MapStateDescriptor<String, String> DESCRIPTOR =
      new MapStateDescriptor<>("ref-data", Types.STRING, Types.STRING);

  private TwoInputStreamOperatorTestHarness<String, String, String> createHarness(
      StaticJoiner.JoinBroadcastFunction<String, String> function) throws Exception {
    var operator = new CoBroadcastWithNonKeyedOperator<>(function, List.of(DESCRIPTOR));
    var harness = new TwoInputStreamOperatorTestHarness<>(operator);
    harness.open();
    return harness;
  }

  private static StaticJoiner.JoinBroadcastFunction<String, String> createFunction() {
    return new StaticJoiner.JoinBroadcastFunction<>(
        event -> event.split(":")[0],
        ref -> ref.split("=")[0],
        (event, refData) -> event + "+" + refData,
        DESCRIPTOR);
  }

  private static List<String> extractOutput(
      TwoInputStreamOperatorTestHarness<String, String, String> harness) {
    return harness.extractOutputStreamRecords().stream().map(r -> (String) r.getValue()).toList();
  }

  @Test
  @DisplayName("should enrich when reference data exists")
  void shouldEnrichWhenReferenceExists() throws Exception {
    var function = createFunction();

    try (var harness = createHarness(function)) {
      harness.processElement2(new StreamRecord<>("user1=Alice"));
      harness.processElement1(new StreamRecord<>("user1:login"));

      List<String> output = extractOutput(harness);
      assertThat(output).containsExactly("user1:login+user1=Alice");
    }
  }

  @Test
  @DisplayName("should pass through when no reference data")
  void shouldPassThroughWhenNoReference() throws Exception {
    var function = createFunction();

    try (var harness = createHarness(function)) {
      harness.processElement1(new StreamRecord<>("user1:login"));

      List<String> output = extractOutput(harness);
      assertThat(output).containsExactly("user1:login");
    }
  }

  @Test
  @DisplayName("should use updated reference data")
  void shouldUseUpdatedReferenceData() throws Exception {
    var function = createFunction();

    try (var harness = createHarness(function)) {
      harness.processElement2(new StreamRecord<>("user1=Alice"));
      harness.processElement2(new StreamRecord<>("user1=Alicia"));
      harness.processElement1(new StreamRecord<>("user1:login"));

      List<String> output = extractOutput(harness);
      assertThat(output).containsExactly("user1:login+user1=Alicia");
    }
  }

  @Test
  @DisplayName("should handle multiple keys independently")
  void shouldHandleMultipleKeys() throws Exception {
    var function = createFunction();

    try (var harness = createHarness(function)) {
      harness.processElement2(new StreamRecord<>("user1=Alice"));
      harness.processElement2(new StreamRecord<>("user2=Bob"));
      harness.processElement1(new StreamRecord<>("user1:login"));
      harness.processElement1(new StreamRecord<>("user2:logout"));

      List<String> output = extractOutput(harness);
      assertThat(output).containsExactly("user1:login+user1=Alice", "user2:logout+user2=Bob");
    }
  }

  @Test
  @DisplayName("should pass through when key does not match any reference")
  void shouldPassThroughWhenKeyMismatch() throws Exception {
    var function = createFunction();

    try (var harness = createHarness(function)) {
      harness.processElement2(new StreamRecord<>("user1=Alice"));
      harness.processElement1(new StreamRecord<>("user2:login"));

      List<String> output = extractOutput(harness);
      assertThat(output).containsExactly("user2:login");
    }
  }

  @Nested
  @DisplayName("Builder validation")
  class BuilderValidation {

    @Test
    @DisplayName("should throw when mainKeyExtractor is null")
    void nullMainKeyExtractorThrows() {
      assertThatThrownBy(
              () ->
                  StaticJoiner.<String, String>builder()
                      .refKeyExtractor(r -> r)
                      .joinFunction((e, r) -> e)
                      .descriptor(DESCRIPTOR)
                      .build())
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("mainKeyExtractor must not be null");
    }

    @Test
    @DisplayName("should throw when refKeyExtractor is null")
    void nullRefKeyExtractorThrows() {
      assertThatThrownBy(
              () ->
                  StaticJoiner.<String, String>builder()
                      .mainKeyExtractor(e -> e)
                      .joinFunction((e, r) -> e)
                      .descriptor(DESCRIPTOR)
                      .build())
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("refKeyExtractor must not be null");
    }

    @Test
    @DisplayName("should throw when joinFunction is null")
    void nullJoinFunctionThrows() {
      assertThatThrownBy(
              () ->
                  StaticJoiner.<String, String>builder()
                      .mainKeyExtractor(e -> e)
                      .refKeyExtractor(r -> r)
                      .descriptor(DESCRIPTOR)
                      .build())
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("joinFunction must not be null");
    }

    @Test
    @DisplayName("should throw when descriptor is null")
    void nullDescriptorThrows() {
      assertThatThrownBy(
              () ->
                  StaticJoiner.<String, String>builder()
                      .mainKeyExtractor(e -> e)
                      .refKeyExtractor(r -> r)
                      .joinFunction((e, r) -> e)
                      .build())
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("descriptor must not be null");
    }
  }
}
