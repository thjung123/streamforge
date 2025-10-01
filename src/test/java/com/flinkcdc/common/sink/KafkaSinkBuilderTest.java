package com.flinkcdc.common.sink;

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

        verify(mockMetrics, times(1)).inc("sink.success_count");
        verify(mockMetrics, never()).inc("sink.error_count");
    }
}
