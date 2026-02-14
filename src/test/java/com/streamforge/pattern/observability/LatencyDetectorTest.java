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
  @DisplayName("should not emit alert when latency is below threshold")
  void noAlertBelowThreshold() throws Exception {
    try (var harness = createHarness()) {
      long now = System.currentTimeMillis();
      harness.processElement(new StreamRecord<>(now));

      var alerts = harness.getSideOutput(LatencyDetector.ALERT_TAG);
      assertThat(alerts).isNullOrEmpty();
    }
  }

  @Test
  @DisplayName("should emit alert when latency exceeds threshold")
  void alertAboveThreshold() throws Exception {
    try (var harness = createHarness()) {
      long old = System.currentTimeMillis() - 10_000L;
      harness.processElement(new StreamRecord<>(old));

      var alerts = harness.getSideOutput(LatencyDetector.ALERT_TAG);
      assertThat(alerts).hasSize(1);
      assertThat(alerts.poll().getValue()).contains("exceeded threshold");
    }
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
      assertThat(harness.getSideOutput(LatencyDetector.ALERT_TAG)).isNullOrEmpty();
    }
  }
}
