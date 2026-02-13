package com.streamforge.connector.mongo;

import com.streamforge.core.config.MetricKeys;
import com.streamforge.core.metric.Metrics;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;

import static org.mockito.Mockito.*;

class MongoSourceBuilderTest {

    @Test
    void testMapIncrementsReadCount() throws Exception {
        // given
        MongoSourceBuilder.MetricCountingMap mapFunction = new MongoSourceBuilder.MetricCountingMap("test-job");
        Metrics mockMetrics = mock(Metrics.class);

        Field field = MongoSourceBuilder.MetricCountingMap.class.getDeclaredField("metrics");
        field.setAccessible(true);
        field.set(mapFunction, mockMetrics);

        Document input = new Document("id", 1);

        // when
        mapFunction.map(input);

        // then
        verify(mockMetrics, times(1)).inc(MetricKeys.SOURCE_READ_COUNT);
        verify(mockMetrics, never()).inc(MetricKeys.SOURCE_ERROR_COUNT);
    }

    @Test
    void testMapIncrementsErrorCountOnFailure() throws Exception {
        // given
        MongoSourceBuilder.MetricCountingMap mapFunction = new MongoSourceBuilder.MetricCountingMap("test-job");
        Metrics mockMetrics = mock(Metrics.class);

        Field field = MongoSourceBuilder.MetricCountingMap.class.getDeclaredField("metrics");
        field.setAccessible(true);
        field.set(mapFunction, mockMetrics);

        doThrow(new RuntimeException("boom")).when(mockMetrics).inc(MetricKeys.SOURCE_READ_COUNT);

        Document input = new Document("id", 2);

        // when
        try {
            mapFunction.map(input);
        } catch (RuntimeException ignored) {}

        // then
        verify(mockMetrics, times(1)).inc(MetricKeys.SOURCE_ERROR_COUNT);
    }
}
