package com.flinkcdc.common.sink;

import com.flinkcdc.common.config.MetricKeys;
import com.flinkcdc.common.metric.Metrics;
import com.flinkcdc.common.model.CdcEnvelop;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.*;

class KafkaSinkBuilderTest {

    @Test
    void testMapIncrementsSuccessMetric() throws Exception {
        KafkaSinkBuilder.MetricsMapFunction mapFunction = new KafkaSinkBuilder.MetricsMapFunction("test-job");

        Metrics mockMetrics = mock(Metrics.class);
        var metricsField = mapFunction.getClass().getDeclaredField("metrics");
        metricsField.setAccessible(true);
        metricsField.set(mapFunction, mockMetrics);

        CdcEnvelop envelop = CdcEnvelop.builder()
                .operation("INSERT")
                .payloadJson("{\"id\":1}")
                .build();

        mapFunction.map(envelop);

        verify(mockMetrics, times(1)).inc(MetricKeys.SINK_SUCCESS_COUNT);
        verify(mockMetrics, never()).inc(MetricKeys.SINK_ERROR_COUNT);
    }

    @Test
    void testMapIncrementsErrorMetricOnFailure() throws Exception {
        KafkaSinkBuilder.MetricsMapFunction mapFunction = new KafkaSinkBuilder.MetricsMapFunction("test-job");

        Metrics mockMetrics = mock(Metrics.class);
        var metricsField = mapFunction.getClass().getDeclaredField("metrics");
        metricsField.setAccessible(true);
        metricsField.set(mapFunction, mockMetrics);

        doThrow(new RuntimeException("boom"))
                .when(mockMetrics).inc(MetricKeys.SINK_SUCCESS_COUNT);

        CdcEnvelop envelop = CdcEnvelop.builder()
                .operation("INSERT")
                .payloadJson("{\"id\":1}")
                .build();

        try {
            mapFunction.map(envelop);
        } catch (RuntimeException ignored) {}

        verify(mockMetrics, times(1)).inc(MetricKeys.SINK_SUCCESS_COUNT); // 시도했으나 실패
        verify(mockMetrics, times(1)).inc(MetricKeys.SINK_ERROR_COUNT);   // 실패로 인해 error 증가
    }
}
