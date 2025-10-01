package com.flinkcdc.common.sink;

import com.flinkcdc.common.metric.Metrics;
import com.flinkcdc.common.model.CdcEnvelop;
import com.flinkcdc.common.utils.JsonUtils;
import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class MongoSinkBuilderTest {

    @Test
    void testInvokeCallsInsertOne() throws Exception {
        // given
        MongoCollection<Document> mockCollection = Mockito.mock(MongoCollection.class);
        MongoSinkBuilder.MongoSinkFunction sink = new MongoSinkBuilder.MongoSinkFunction("test-job", mockCollection);

        Metrics mockMetrics = mock(Metrics.class);
        var metricsField = sink.getClass().getDeclaredField("metrics");
        metricsField.setAccessible(true);
        metricsField.set(sink, mockMetrics);

        String payloadJson = JsonUtils.toJson(Map.of("id", 1, "name", "Charlie"));
        CdcEnvelop envelop = CdcEnvelop.builder()
                .operation("INSERT")
                .payloadJson(payloadJson)
                .build();

        // when
        sink.invoke(envelop, null);

        // then
        verify(mockCollection, times(1)).insertOne(any(Document.class));
        verify(mockMetrics, times(1)).inc("sink.success_count");
        verify(mockMetrics, never()).inc("sink.error_count");
    }

    @Test
    void testInvokeIncrementsErrorMetricOnFailure() throws Exception {
        // given
        MongoCollection<Document> mockCollection = Mockito.mock(MongoCollection.class);
        MongoSinkBuilder.MongoSinkFunction sink = new MongoSinkBuilder.MongoSinkFunction("test-job", mockCollection);

        Metrics mockMetrics = mock(Metrics.class);
        var metricsField = sink.getClass().getDeclaredField("metrics");
        metricsField.setAccessible(true);
        metricsField.set(sink, mockMetrics);

        doThrow(new RuntimeException("DB insert failed"))
                .when(mockCollection).insertOne(any(Document.class));

        String payloadJson = JsonUtils.toJson(Map.of("id", 2, "name", "Alice"));
        CdcEnvelop envelop = CdcEnvelop.builder()
                .operation("INSERT")
                .payloadJson(payloadJson)
                .build();

        // when
        try {
            sink.invoke(envelop, null);
        } catch (RuntimeException ignored) {

        }

        // then
        verify(mockCollection, times(1)).insertOne(any(Document.class));
        verify(mockMetrics, times(1)).inc("sink.error_count");
        verify(mockMetrics, never()).inc("sink.success_count");
    }
}
