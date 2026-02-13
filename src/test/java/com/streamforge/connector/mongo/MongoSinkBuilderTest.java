package com.streamforge.connector.mongo;

import com.streamforge.core.config.MetricKeys;
import com.streamforge.core.dlq.DLQPublisher;
import com.streamforge.core.metric.Metrics;
import com.streamforge.core.model.CdcEnvelop;
import com.streamforge.core.util.JsonUtils;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.ReplaceOptions;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class MongoSinkBuilderTest {

    @BeforeAll
    static void setupMockDlqPublisher() throws Exception {
        DLQPublisher mockDlq = mock(DLQPublisher.class);
        Field instanceField = DLQPublisher.class.getDeclaredField("instance");
        instanceField.setAccessible(true);
        instanceField.set(null, mockDlq);
    }

    @Test
    void testInvokeCallsReplaceOne() throws Exception {
        // given
        MongoCollection<Document> mockCollection = Mockito.mock(MongoCollection.class);
        MongoSinkBuilder.MongoSinkFunction sink =
                new MongoSinkBuilder.MongoSinkFunction("test-job", mockCollection);

        Metrics mockMetrics = mock(Metrics.class);
        var metricsField = sink.getClass().getDeclaredField("metrics");
        metricsField.setAccessible(true);
        metricsField.set(sink, mockMetrics);

        String payloadJson = JsonUtils.toJson(Map.of("id", 1, "name", "Charlie"));
        CdcEnvelop envelop = CdcEnvelop.builder()
                .operation("INSERT")
                .primaryKey("id")
                .payloadJson(payloadJson)
                .build();

        // when
        sink.invoke(envelop, null);

        // then
        verify(mockCollection, times(1))
                .replaceOne(any(Bson.class), any(Document.class), any(ReplaceOptions.class));
        verify(mockMetrics, times(1)).inc(MetricKeys.SINK_SUCCESS_COUNT);
        verify(mockMetrics, never()).inc(MetricKeys.SINK_ERROR_COUNT);
    }

    @Test
    void testInvokeIncrementsErrorMetricOnFailure() throws Exception {
        // given
        MongoCollection<Document> mockCollection = Mockito.mock(MongoCollection.class);
        MongoSinkBuilder.MongoSinkFunction sink =
                new MongoSinkBuilder.MongoSinkFunction("test-job", mockCollection);

        Metrics mockMetrics = mock(Metrics.class);
        var metricsField = sink.getClass().getDeclaredField("metrics");
        metricsField.setAccessible(true);
        metricsField.set(sink, mockMetrics);

        doThrow(new RuntimeException("DB replace failed"))
                .when(mockCollection)
                .replaceOne(any(Bson.class), any(Document.class), any(ReplaceOptions.class));

        String payloadJson = JsonUtils.toJson(Map.of("id", 2, "name", "Alice"));
        CdcEnvelop envelop = CdcEnvelop.builder()
                .operation("INSERT")
                .primaryKey("id")
                .payloadJson(payloadJson)
                .build();

        // when
        sink.invoke(envelop, null);

        // then
        verify(mockCollection, times(1))
                .replaceOne(any(Bson.class), any(Document.class), any(ReplaceOptions.class));
        verify(mockMetrics, times(1)).inc(MetricKeys.SINK_ERROR_COUNT);
        verify(mockMetrics, never()).inc(MetricKeys.SINK_SUCCESS_COUNT);
    }
}
