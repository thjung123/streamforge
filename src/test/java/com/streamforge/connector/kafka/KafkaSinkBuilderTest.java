package com.streamforge.connector.kafka;

import static org.mockito.Mockito.*;

import com.streamforge.core.config.MetricKeys;
import com.streamforge.core.metric.Metrics;
import com.streamforge.core.model.CdcEnvelop;
import org.junit.jupiter.api.Test;

class KafkaSinkBuilderTest {

  @Test
  void testMapIncrementsSuccessMetric() throws Exception {
    KafkaSinkBuilder.MetricsMapFunction mapFunction =
        new KafkaSinkBuilder.MetricsMapFunction("test-job");

    Metrics mockMetrics = mock(Metrics.class);
    var metricsField = mapFunction.getClass().getDeclaredField("metrics");
    metricsField.setAccessible(true);
    metricsField.set(mapFunction, mockMetrics);

    CdcEnvelop envelop = CdcEnvelop.builder().operation("INSERT").payloadJson("{\"id\":1}").build();

    mapFunction.map(envelop);

    verify(mockMetrics, times(1)).inc(MetricKeys.SINK_SUCCESS_COUNT);
    verify(mockMetrics, never()).inc(MetricKeys.SINK_ERROR_COUNT);
  }

  @Test
  void testMapIncrementsErrorMetricOnFailure() throws Exception {
    KafkaSinkBuilder.MetricsMapFunction mapFunction =
        new KafkaSinkBuilder.MetricsMapFunction("test-job");

    Metrics mockMetrics = mock(Metrics.class);
    var metricsField = mapFunction.getClass().getDeclaredField("metrics");
    metricsField.setAccessible(true);
    metricsField.set(mapFunction, mockMetrics);

    doThrow(new RuntimeException("boom")).when(mockMetrics).inc(MetricKeys.SINK_SUCCESS_COUNT);

    CdcEnvelop envelop = CdcEnvelop.builder().operation("INSERT").payloadJson("{\"id\":1}").build();

    try {
      mapFunction.map(envelop);
    } catch (RuntimeException ignored) {
    }

    verify(mockMetrics, times(1)).inc(MetricKeys.SINK_SUCCESS_COUNT);
    verify(mockMetrics, times(1)).inc(MetricKeys.SINK_ERROR_COUNT);
  }
}
