package com.streamforge.pattern.observability;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import com.streamforge.core.config.MetricKeys;
import com.streamforge.core.metric.Metrics;
import java.util.List;
import java.util.Objects;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class OnlineObserverTest {

  private Metrics metrics;
  private OnlineObserver.ObserverFunction<String> function;

  @BeforeEach
  void setUp() {
    metrics = mock(Metrics.class);
    function =
        new OnlineObserver.ObserverFunction<>(
            List.of(
                QualityCheck.of("null_check", Objects::isNull),
                QualityCheck.of("empty_check", v -> v != null && v.isEmpty())),
            "test",
            metrics);
  }

  @Test
  @DisplayName("should count total events")
  void countTotal() {
    function.map("hello");
    function.map("world");

    verify(metrics, times(2)).inc(MetricKeys.OBSERVER_TOTAL_COUNT);
  }

  @Test
  @DisplayName("should increment check counter when condition matches")
  void countQualityIssue() {
    function.map("");

    verify(metrics).inc(MetricKeys.OBSERVER_TOTAL_COUNT);
    verify(metrics).inc("empty_check");
    verify(metrics, never()).inc("null_check");
  }

  @Test
  @DisplayName("should not increment check counters for valid data")
  void noCountForValidData() {
    function.map("valid");

    verify(metrics).inc(MetricKeys.OBSERVER_TOTAL_COUNT);
    verify(metrics, never()).inc("null_check");
    verify(metrics, never()).inc("empty_check");
  }

  @Test
  @DisplayName("should pass all events through unchanged")
  void passThrough() {
    String result = function.map("test");

    assertThat(result).isEqualTo("test");
  }
}
