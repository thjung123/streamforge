package com.flinkcdc.common.source;

import com.flinkcdc.common.config.MetricKeys;
import com.flinkcdc.common.metric.Metrics;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.*;

class KafkaSourceBuilderTest {

    private KafkaSourceBuilder.MetricCountingMap mapFunction;
    private Metrics mockMetrics;

    @BeforeEach
    void setUp() throws Exception {
        mapFunction = new KafkaSourceBuilder.MetricCountingMap("test-job");

        mockMetrics = mock(Metrics.class);

        var field = KafkaSourceBuilder.MetricCountingMap.class.getDeclaredField("metrics");
        field.setAccessible(true);
        field.set(mapFunction, mockMetrics);
    }

    @Test
    void testMapIncrementsReadMetricOnSuccess() throws Exception {
        // when
        String result = mapFunction.map("test-message");

        // then
        verify(mockMetrics, times(1)).inc(MetricKeys.SOURCE_READ_COUNT);
        verify(mockMetrics, never()).inc(MetricKeys.SOURCE_ERROR_COUNT);
        assert result.equals("test-message");
    }

    @Test
    void testMapIncrementsErrorMetricOnFailure() throws Exception {
        // given
        Metrics mockMetrics = mock(Metrics.class);
        KafkaSourceBuilder.MetricCountingMap mapFunction = new KafkaSourceBuilder.MetricCountingMap("test-job");

        var field = KafkaSourceBuilder.MetricCountingMap.class.getDeclaredField("metrics");
        field.setAccessible(true);
        field.set(mapFunction, mockMetrics);

        doThrow(new RuntimeException("boom"))
                .when(mockMetrics).inc(MetricKeys.SOURCE_READ_COUNT);

        // when
        try {
            mapFunction.map("test-value");
        } catch (RuntimeException ignored) {}

        // then
        verify(mockMetrics, times(1)).inc(MetricKeys.SOURCE_ERROR_COUNT);
    }

}
