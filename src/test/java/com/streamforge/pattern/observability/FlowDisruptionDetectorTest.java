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
  @DisplayName("should emit disruption alert after timeout")
  void disruptionAlert() throws Exception {
    try (var harness = createHarness()) {
      harness.processElement(new StreamRecord<>("a", 1L));

      harness.setProcessingTime(harness.getProcessingTime() + TIMEOUT.toMillis() + 1);

      var alerts = harness.getSideOutput(FlowDisruptionDetector.ALERT_TAG);
      assertThat(alerts).hasSize(1);
      assertThat(alerts.poll().getValue()).contains("No events for");
    }
  }

  @Test
  @DisplayName("should not emit alert if events keep arriving")
  void noAlertWhenActive() throws Exception {
    try (var harness = createHarness()) {
      harness.processElement(new StreamRecord<>("a", 1L));

      harness.setProcessingTime(harness.getProcessingTime() + 5_000L);
      harness.processElement(new StreamRecord<>("b", 2L));

      harness.setProcessingTime(harness.getProcessingTime() + 5_000L);
      harness.processElement(new StreamRecord<>("c", 3L));

      var alerts = harness.getSideOutput(FlowDisruptionDetector.ALERT_TAG);
      assertThat(alerts).isNullOrEmpty();
    }
  }

  @Test
  @DisplayName("should emit recovery alert when events resume after disruption")
  void recoveryAlert() throws Exception {
    try (var harness = createHarness()) {
      harness.processElement(new StreamRecord<>("a", 1L));

      harness.setProcessingTime(harness.getProcessingTime() + TIMEOUT.toMillis() + 1);

      var disruptionAlerts = harness.getSideOutput(FlowDisruptionDetector.ALERT_TAG);
      assertThat(disruptionAlerts).hasSize(1);
      disruptionAlerts.clear();

      harness.processElement(new StreamRecord<>("b", 2L));

      var recoveryAlerts = harness.getSideOutput(FlowDisruptionDetector.ALERT_TAG);
      assertThat(recoveryAlerts).hasSize(1);
      assertThat(recoveryAlerts.poll().getValue()).contains("recovered");
    }
  }
}
