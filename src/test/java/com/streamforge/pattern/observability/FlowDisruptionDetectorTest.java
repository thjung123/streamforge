package com.streamforge.pattern.observability;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class FlowDisruptionDetectorTest {

  private static final Duration TIMEOUT = Duration.ofSeconds(10);
  private static final String KEY = "source-1";

  private KeyedOneInputStreamOperatorTestHarness<String, String, String> createHarness()
      throws Exception {
    var function = new FlowDisruptionDetector.DisruptionFunction<String>(TIMEOUT, "test");
    var operator = new KeyedProcessOperator<>(function);
    var harness = new KeyedOneInputStreamOperatorTestHarness<>(operator, v -> KEY, Types.STRING);
    harness.open();
    return harness;
  }

  @Test
  @DisplayName("should pass all events through")
  void passThrough() throws Exception {
    try (var harness = createHarness()) {
      harness.processElement(new StreamRecord<>("a", 1L));
      harness.processElement(new StreamRecord<>("b", 2L));

      List<String> output =
          harness.extractOutputStreamRecords().stream().map(r -> (String) r.getValue()).toList();

      assertThat(output).containsExactly("a", "b");
    }
  }

  @Test
  @DisplayName("should keep a single inactivity timer, resetting it on each event")
  void resetsTimerPerElement() throws Exception {
    try (var harness = createHarness()) {
      harness.processElement(new StreamRecord<>("a", 1L));
      assertThat(harness.numProcessingTimeTimers()).isEqualTo(1);

      harness.setProcessingTime(harness.getProcessingTime() + 5_000L);
      harness.processElement(new StreamRecord<>("b", 2L));

      // old timer deleted, new one registered — still exactly one
      assertThat(harness.numProcessingTimeTimers()).isEqualTo(1);
    }
  }

  @Test
  @DisplayName("should fire and clear the timer after the inactivity timeout")
  void firesTimerAfterTimeout() throws Exception {
    try (var harness = createHarness()) {
      harness.processElement(new StreamRecord<>("a", 1L));
      assertThat(harness.numProcessingTimeTimers()).isEqualTo(1);

      harness.setProcessingTime(harness.getProcessingTime() + TIMEOUT.toMillis() + 1);

      assertThat(harness.numProcessingTimeTimers()).isZero();
    }
  }

  @Test
  @DisplayName("should re-arm the timer when events resume after a disruption")
  void reArmsAfterDisruption() throws Exception {
    try (var harness = createHarness()) {
      harness.processElement(new StreamRecord<>("a", 1L));
      harness.setProcessingTime(harness.getProcessingTime() + TIMEOUT.toMillis() + 1);
      assertThat(harness.numProcessingTimeTimers()).isZero();

      harness.processElement(new StreamRecord<>("b", 2L));
      assertThat(harness.numProcessingTimeTimers()).isEqualTo(1);

      List<String> output =
          harness.extractOutputStreamRecords().stream().map(r -> (String) r.getValue()).toList();
      assertThat(output).containsExactly("a", "b");
    }
  }
}
