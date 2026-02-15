package com.streamforge.pattern.observability;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.apache.flink.streaming.api.operators.ProcessOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class LateDataDetectorTest {

  private static final Duration ALLOWED_LATENESS = Duration.ofSeconds(5);

  private OneInputStreamOperatorTestHarness<Long, Long> createHarness() throws Exception {
    var function =
        new LateDataDetector.LateDataFunction<>(Instant::ofEpochMilli, ALLOWED_LATENESS, "test");
    var operator = new ProcessOperator<>(function);
    var harness = new OneInputStreamOperatorTestHarness<>(operator);
    harness.open();
    return harness;
  }

  @Test
  @DisplayName("should pass all events through regardless of lateness")
  void passThrough() throws Exception {
    try (var harness = createHarness()) {
      long now = System.currentTimeMillis();
      harness.processWatermark(new Watermark(now));
      harness.processElement(new StreamRecord<>(now));
      harness.processElement(new StreamRecord<>(now - 10_000L));

      List<Long> output =
          harness.extractOutputStreamRecords().stream().map(r -> (Long) r.getValue()).toList();

      assertThat(output).containsExactly(now, now - 10_000L);
    }
  }

  @Test
  @DisplayName("should not emit alert when event is within allowed lateness")
  void withinLateness() throws Exception {
    try (var harness = createHarness()) {
      long now = System.currentTimeMillis();
      harness.processWatermark(new Watermark(now));
      harness.processElement(new StreamRecord<>(now - 3_000L));

      var alerts = harness.getSideOutput(LateDataDetector.ALERT_TAG);
      assertThat(alerts).isNullOrEmpty();
    }
  }

  @Test
  @DisplayName("should emit alert when event exceeds allowed lateness")
  void exceedsLateness() throws Exception {
    try (var harness = createHarness()) {
      long now = System.currentTimeMillis();
      harness.processWatermark(new Watermark(now));
      harness.processElement(new StreamRecord<>(now - 10_000L));

      var alerts = harness.getSideOutput(LateDataDetector.ALERT_TAG);
      assertThat(alerts).hasSize(1);
      assertThat(alerts.poll().getValue()).contains("Late event");
    }
  }

  @Test
  @DisplayName("should not emit alert when no watermark received yet")
  void noWatermark() throws Exception {
    try (var harness = createHarness()) {
      long old = System.currentTimeMillis() - 100_000L;
      harness.processElement(new StreamRecord<>(old));

      var alerts = harness.getSideOutput(LateDataDetector.ALERT_TAG);
      assertThat(alerts).isNullOrEmpty();
    }
  }
}
