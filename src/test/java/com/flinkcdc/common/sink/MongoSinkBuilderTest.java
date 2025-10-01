package com.flinkcdc.common.sink;

import com.flinkcdc.common.metric.Metrics;
import com.flinkcdc.common.model.CdcEnvelop;
import com.flinkcdc.common.utils.JsonUtils;
import com.mongodb.client.MongoCollection;
import org.apache.flink.configuration.Configuration;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class MongoSinkBuilderTest {

    @Test
    void testInvokeCallsInsertOne() throws Exception {
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

        sink.invoke(envelop, null);

        verify(mockCollection, times(1)).insertOne(any(Document.class));
        verify(mockMetrics, times(1)).inc("sink.success_count");
    }

}
