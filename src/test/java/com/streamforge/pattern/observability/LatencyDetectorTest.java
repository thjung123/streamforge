package com.streamforge.pattern.observability;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class LatencyDetectorTest {

  private static final Duration THRESHOLD = Duration.ofSeconds(5);

  private OneInputStreamOperatorTestHarness<Long, Long> createHarness() throws Exception {
    var function = new LatencyDetector.LatencyFunction<>(Instant::ofEpochMilli, THRESHOLD, "test");
    var operator = new ProcessOperator<>(function);
    var harness = new OneInputStreamOperatorTestHarness<>(operator);
    harness.open();
    return harness;
  }

  @Test
  @DisplayName("should pass all events through regardless of latency")
  void passThrough() throws Exception {
    try (var harness = createHarness()) {
      long now = System.currentTimeMillis();
      harness.processElement(new StreamRecord<>(now));
      harness.processElement(new StreamRecord<>(now - 10_000L));

      List<Long> output =
          harness.extractOutputStreamRecords().stream().map(r -> (Long) r.getValue()).toList();

      assertThat(output).containsExactly(now, now - 10_000L);
    }
  }

  @Test
  @DisplayName("should flag latency above the threshold")
  void aboveThreshold() {
    assertThat(LatencyDetector.exceedsThreshold(6_000L, THRESHOLD)).isTrue();
  }

  @Test
  @DisplayName("should not flag latency at or below the threshold")
  void withinThreshold() {
    assertThat(LatencyDetector.exceedsThreshold(5_000L, THRESHOLD)).isFalse();
    assertThat(LatencyDetector.exceedsThreshold(4_000L, THRESHOLD)).isFalse();
  }

  @Test
  @DisplayName("should handle null eventTime gracefully")
  void nullEventTime() throws Exception {
    var function = new LatencyDetector.LatencyFunction<String>(s -> null, THRESHOLD, "test");
    var operator = new ProcessOperator<>(function);
    try (var harness = new OneInputStreamOperatorTestHarness<>(operator)) {
      harness.open();
      harness.processElement(new StreamRecord<>("test"));

      List<String> output =
          harness.extractOutputStreamRecords().stream().map(r -> (String) r.getValue()).toList();

      assertThat(output).containsExactly("test");
    }
  }
}
