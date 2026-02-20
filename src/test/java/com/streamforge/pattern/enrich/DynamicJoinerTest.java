package com.streamforge.pattern.enrich;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.co.KeyedCoProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class DynamicJoinerTest {

  private static final Duration TTL = Duration.ofMinutes(10);

  private KeyedTwoInputStreamOperatorTestHarness<String, String, String, String> createHarness(
      DynamicJoiner.DynamicJoinFunction<String, String> function) throws Exception {
    var operator = new KeyedCoProcessOperator<>(function);
    var harness =
        new KeyedTwoInputStreamOperatorTestHarness<>(
            operator, e -> e.split(":")[0], r -> r.split("=")[0], Types.STRING);
    harness.open();
    return harness;
  }

  private static DynamicJoiner.DynamicJoinFunction<String, String> createFunction(
      DynamicJoiner.JoinType joinType) {
    return createFunction(joinType, null);
  }

  private static DynamicJoiner.DynamicJoinFunction<String, String> createFunction(
      DynamicJoiner.JoinType joinType,
      DynamicJoiner.RightEmitFunction<String, String> rightEmitFunction) {
    return new DynamicJoiner.DynamicJoinFunction<>(
        (left, right) -> left + "+" + right, joinType, TTL, rightEmitFunction);
  }

  private static List<String> extractOutput(
      KeyedTwoInputStreamOperatorTestHarness<String, String, String, String> harness) {
    return harness.extractOutputStreamRecords().stream().map(r -> (String) r.getValue()).toList();
  }

  @Nested
  @DisplayName("INNER join")
  class InnerJoin {

    @Test
    @DisplayName("should emit join result when both sides match")
    void shouldEmitWhenBothMatch() throws Exception {
      var function = createFunction(DynamicJoiner.JoinType.INNER);

      try (var harness = createHarness(function)) {
        harness.processElement1(new StreamRecord<>("user1:login"));
        harness.processElement2(new StreamRecord<>("user1=Alice"));

        List<String> output = extractOutput(harness);
        assertThat(output).containsExactly("user1:login+user1=Alice");
      }
    }

    @Test
    @DisplayName("should not emit when only left arrives")
    void shouldNotEmitWhenOnlyLeft() throws Exception {
      var function = createFunction(DynamicJoiner.JoinType.INNER);

      try (var harness = createHarness(function)) {
        harness.processElement1(new StreamRecord<>("user1:login"));

        List<String> output = extractOutput(harness);
        assertThat(output).isEmpty();
      }
    }

    @Test
    @DisplayName("should not emit when only right arrives")
    void shouldNotEmitWhenOnlyRight() throws Exception {
      var function = createFunction(DynamicJoiner.JoinType.INNER);

      try (var harness = createHarness(function)) {
        harness.processElement2(new StreamRecord<>("user1=Alice"));

        List<String> output = extractOutput(harness);
        assertThat(output).isEmpty();
      }
    }

    @Test
    @DisplayName("should join late right with buffered left")
    void shouldJoinLateRightWithBufferedLeft() throws Exception {
      var function = createFunction(DynamicJoiner.JoinType.INNER);

      try (var harness = createHarness(function)) {
        harness.processElement1(new StreamRecord<>("user1:login"));
        harness.processElement1(new StreamRecord<>("user1:click"));
        harness.processElement2(new StreamRecord<>("user1=Alice"));

        List<String> output = extractOutput(harness);
        assertThat(output).containsExactly("user1:login+user1=Alice", "user1:click+user1=Alice");
      }
    }

    @Test
    @DisplayName("should handle N:M matching within TTL")
    void shouldHandleNtoMMatching() throws Exception {
      var function = createFunction(DynamicJoiner.JoinType.INNER);

      try (var harness = createHarness(function)) {
        harness.processElement1(new StreamRecord<>("user1:login"));
        harness.processElement2(new StreamRecord<>("user1=Alice"));
        harness.processElement1(new StreamRecord<>("user1:click"));
        harness.processElement2(new StreamRecord<>("user1=Bob"));

        List<String> output = extractOutput(harness);
        assertThat(output)
            .containsExactly(
                "user1:login+user1=Alice",
                "user1:click+user1=Alice",
                "user1:login+user1=Bob",
                "user1:click+user1=Bob");
      }
    }

    @Test
    @DisplayName("should handle different keys independently")
    void shouldHandleDifferentKeysIndependently() throws Exception {
      var function = createFunction(DynamicJoiner.JoinType.INNER);

      try (var harness = createHarness(function)) {
        harness.processElement1(new StreamRecord<>("user1:login"));
        harness.processElement1(new StreamRecord<>("user2:login"));
        harness.processElement2(new StreamRecord<>("user1=Alice"));

        List<String> output = extractOutput(harness);
        assertThat(output).containsExactly("user1:login+user1=Alice");
      }
    }
  }

  @Nested
  @DisplayName("LEFT join")
  class LeftJoin {

    @Test
    @DisplayName("should emit join result when matched")
    void shouldEmitWhenMatched() throws Exception {
      var function = createFunction(DynamicJoiner.JoinType.LEFT);

      try (var harness = createHarness(function)) {
        harness.processElement1(new StreamRecord<>("user1:login"));
        harness.processElement2(new StreamRecord<>("user1=Alice"));

        List<String> output = extractOutput(harness);
        assertThat(output).containsExactly("user1:login+user1=Alice");
      }
    }

    @Test
    @DisplayName("should emit unmatched left event on timer expiry")
    void shouldEmitUnmatchedLeftOnExpiry() throws Exception {
      var function = createFunction(DynamicJoiner.JoinType.LEFT);

      try (var harness = createHarness(function)) {
        harness.setProcessingTime(0L);
        harness.processElement1(new StreamRecord<>("user1:login"));

        // Advance processing time past TTL
        harness.setProcessingTime(TTL.toMillis());

        List<String> output = extractOutput(harness);
        assertThat(output).containsExactly("user1:login");
      }
    }

    @Test
    @DisplayName("should not emit unmatched right event")
    void shouldNotEmitUnmatchedRight() throws Exception {
      var function = createFunction(DynamicJoiner.JoinType.LEFT);

      try (var harness = createHarness(function)) {
        harness.setProcessingTime(0L);
        harness.processElement2(new StreamRecord<>("user1=Alice"));

        harness.setProcessingTime(TTL.toMillis());

        List<String> output = extractOutput(harness);
        assertThat(output).isEmpty();
      }
    }
  }

  @Nested
  @DisplayName("RIGHT join")
  class RightJoin {

    @Test
    @DisplayName("should emit join result when matched")
    void shouldEmitWhenMatched() throws Exception {
      var function = createFunction(DynamicJoiner.JoinType.RIGHT, r -> "UNMATCHED:" + r);

      try (var harness = createHarness(function)) {
        harness.processElement1(new StreamRecord<>("user1:login"));
        harness.processElement2(new StreamRecord<>("user1=Alice"));

        List<String> output = extractOutput(harness);
        assertThat(output).containsExactly("user1:login+user1=Alice");
      }
    }

    @Test
    @DisplayName("should emit unmatched right via rightEmitFunction on expiry")
    void shouldEmitUnmatchedRightOnExpiry() throws Exception {
      var function = createFunction(DynamicJoiner.JoinType.RIGHT, r -> "UNMATCHED:" + r);

      try (var harness = createHarness(function)) {
        harness.setProcessingTime(0L);
        harness.processElement2(new StreamRecord<>("user1=Alice"));

        harness.setProcessingTime(TTL.toMillis());

        List<String> output = extractOutput(harness);
        assertThat(output).containsExactly("UNMATCHED:user1=Alice");
      }
    }

    @Test
    @DisplayName("should not emit unmatched left event")
    void shouldNotEmitUnmatchedLeft() throws Exception {
      var function = createFunction(DynamicJoiner.JoinType.RIGHT, r -> "UNMATCHED:" + r);

      try (var harness = createHarness(function)) {
        harness.setProcessingTime(0L);
        harness.processElement1(new StreamRecord<>("user1:login"));

        harness.setProcessingTime(TTL.toMillis());

        List<String> output = extractOutput(harness);
        assertThat(output).isEmpty();
      }
    }
  }

  @Nested
  @DisplayName("FULL_OUTER join")
  class FullOuterJoin {

    @Test
    @DisplayName("should emit both unmatched sides on expiry")
    void shouldEmitBothUnmatchedOnExpiry() throws Exception {
      var function = createFunction(DynamicJoiner.JoinType.FULL_OUTER, r -> "UNMATCHED:" + r);

      try (var harness = createHarness(function)) {
        harness.setProcessingTime(0L);
        harness.processElement1(new StreamRecord<>("user1:login"));
        harness.processElement2(new StreamRecord<>("user2=Bob"));

        harness.setProcessingTime(TTL.toMillis());

        List<String> output = extractOutput(harness);
        assertThat(output).containsExactlyInAnyOrder("user1:login", "UNMATCHED:user2=Bob");
      }
    }

    @Test
    @DisplayName("should not emit matched events on expiry")
    void shouldNotEmitMatchedOnExpiry() throws Exception {
      var function = createFunction(DynamicJoiner.JoinType.FULL_OUTER, r -> "UNMATCHED:" + r);

      try (var harness = createHarness(function)) {
        harness.setProcessingTime(0L);
        harness.processElement1(new StreamRecord<>("user1:login"));
        harness.processElement2(new StreamRecord<>("user1=Alice"));

        harness.setProcessingTime(TTL.toMillis());

        List<String> output = extractOutput(harness);
        assertThat(output).containsExactly("user1:login+user1=Alice");
      }
    }
  }

  @Nested
  @DisplayName("Builder validation")
  class BuilderValidation {

    @Test
    @DisplayName("should throw when leftKeyExtractor is null")
    void nullLeftKeyExtractorThrows() {
      assertThatThrownBy(
              () ->
                  DynamicJoiner.<String, String>builder()
                      .rightKeyExtractor(r -> r)
                      .joinFunction((l, r) -> l)
                      .build())
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("leftKeyExtractor must not be null");
    }

    @Test
    @DisplayName("should throw when rightKeyExtractor is null")
    void nullRightKeyExtractorThrows() {
      assertThatThrownBy(
              () ->
                  DynamicJoiner.<String, String>builder()
                      .leftKeyExtractor(e -> e)
                      .joinFunction((l, r) -> l)
                      .build())
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("rightKeyExtractor must not be null");
    }

    @Test
    @DisplayName("should throw when joinFunction is null")
    void nullJoinFunctionThrows() {
      assertThatThrownBy(
              () ->
                  DynamicJoiner.<String, String>builder()
                      .leftKeyExtractor(e -> e)
                      .rightKeyExtractor(r -> r)
                      .build())
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("joinFunction must not be null");
    }

    @Test
    @DisplayName("should throw when rightEmitFunction is null for RIGHT join")
    void nullRightEmitFunctionForRightJoinThrows() {
      assertThatThrownBy(
              () ->
                  DynamicJoiner.<String, String>builder()
                      .leftKeyExtractor(e -> e)
                      .rightKeyExtractor(r -> r)
                      .joinFunction((l, r) -> l)
                      .joinType(DynamicJoiner.JoinType.RIGHT)
                      .build())
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("rightEmitFunction must not be null");
    }

    @Test
    @DisplayName("should throw when rightEmitFunction is null for FULL_OUTER join")
    void nullRightEmitFunctionForFullOuterJoinThrows() {
      assertThatThrownBy(
              () ->
                  DynamicJoiner.<String, String>builder()
                      .leftKeyExtractor(e -> e)
                      .rightKeyExtractor(r -> r)
                      .joinFunction((l, r) -> l)
                      .joinType(DynamicJoiner.JoinType.FULL_OUTER)
                      .build())
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("rightEmitFunction must not be null");
    }

    @Test
    @DisplayName("should build RIGHT join with rightEmitFunction")
    void shouldBuildRightJoinWithRightEmitFunction() {
      DynamicJoiner<String, String> joiner =
          DynamicJoiner.<String, String>builder()
              .leftKeyExtractor(e -> e)
              .rightKeyExtractor(r -> r)
              .joinFunction((l, r) -> l)
              .joinType(DynamicJoiner.JoinType.RIGHT)
              .rightEmitFunction(r -> r)
              .build();
      assertThat(joiner).isNotNull();
    }

    @Test
    @DisplayName("should not require rightEmitFunction for INNER join")
    void innerJoinDoesNotRequireRightEmitFunction() {
      DynamicJoiner<String, String> joiner =
          DynamicJoiner.<String, String>builder()
              .leftKeyExtractor(e -> e)
              .rightKeyExtractor(r -> r)
              .joinFunction((l, r) -> l)
              .joinType(DynamicJoiner.JoinType.INNER)
              .build();
      assertThat(joiner).isNotNull();
    }

    @Test
    @DisplayName("should not require rightEmitFunction for LEFT join")
    void leftJoinDoesNotRequireRightEmitFunction() {
      DynamicJoiner<String, String> joiner =
          DynamicJoiner.<String, String>builder()
              .leftKeyExtractor(e -> e)
              .rightKeyExtractor(r -> r)
              .joinFunction((l, r) -> l)
              .joinType(DynamicJoiner.JoinType.LEFT)
              .build();
      assertThat(joiner).isNotNull();
    }
  }

  @Nested
  @DisplayName("Edge cases")
  class EdgeCases {

    @Test
    @DisplayName("should handle multiple late arrivals on both sides")
    void shouldHandleMultipleLateArrivals() throws Exception {
      var function = createFunction(DynamicJoiner.JoinType.INNER);

      try (var harness = createHarness(function)) {
        harness.processElement2(new StreamRecord<>("user1=Alice"));
        harness.processElement2(new StreamRecord<>("user1=Bob"));
        harness.processElement1(new StreamRecord<>("user1:login"));

        List<String> output = extractOutput(harness);
        assertThat(output).containsExactly("user1:login+user1=Alice", "user1:login+user1=Bob");
      }
    }
  }
}
