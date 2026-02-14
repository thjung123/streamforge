package com.streamforge.connector.kafka;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.streamforge.core.config.MetricKeys;
import com.streamforge.core.dlq.DLQPublisher;
import com.streamforge.core.metric.Metrics;
import com.streamforge.core.model.StreamEnvelop;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class KafkaSinkBuilderTest {

  private Metrics metrics;
  private DLQPublisher dlqPublisher;
  private KafkaSinkBuilder.MetricsMapFunction mapFunction;

  @BeforeEach
  void setUp() {
    metrics = mock(Metrics.class);
    dlqPublisher = mock(DLQPublisher.class);
    mapFunction = new KafkaSinkBuilder.MetricsMapFunction("test-job", metrics, dlqPublisher);
  }

  @Test
  void testMapIncrementsSuccessMetric() {
    StreamEnvelop envelop =
        StreamEnvelop.builder().operation("INSERT").payloadJson("{\"id\":1}").build();

    StreamEnvelop result = mapFunction.map(envelop);

    assertSame(envelop, result);
    verify(metrics).inc(MetricKeys.SINK_SUCCESS_COUNT);
    verify(metrics, never()).inc(MetricKeys.SINK_ERROR_COUNT);
    verifyNoInteractions(dlqPublisher);
  }

  @Test
  void testMapIncrementsErrorMetricOnFailure() {
    doThrow(new RuntimeException("boom")).when(metrics).inc(MetricKeys.SINK_SUCCESS_COUNT);

    StreamEnvelop envelop =
        StreamEnvelop.builder().operation("INSERT").payloadJson("{\"id\":1}").build();

    assertThrows(RuntimeException.class, () -> mapFunction.map(envelop));

    verify(metrics).inc(MetricKeys.SINK_SUCCESS_COUNT);
    verify(metrics).inc(MetricKeys.SINK_ERROR_COUNT);
    verify(dlqPublisher).publish(any());
  }
}
