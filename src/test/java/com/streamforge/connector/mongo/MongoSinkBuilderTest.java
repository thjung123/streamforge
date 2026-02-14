package com.streamforge.connector.mongo;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.ReplaceOptions;
import com.streamforge.core.config.MetricKeys;
import com.streamforge.core.dlq.DLQPublisher;
import com.streamforge.core.metric.Metrics;
import com.streamforge.core.model.StreamEnvelop;
import com.streamforge.core.util.JsonUtils;
import java.lang.reflect.Field;
import java.util.Map;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class MongoSinkBuilderTest {

  @BeforeAll
  static void setupMockDlqPublisher() throws Exception {
    DLQPublisher mockDlq = mock(DLQPublisher.class);
    Field instanceField = DLQPublisher.class.getDeclaredField("instance");
    instanceField.setAccessible(true);
    instanceField.set(null, mockDlq);
  }

  @SuppressWarnings("unchecked")
  @Test
  void testInvokeCallsReplaceOne() throws Exception {
    // given
    MongoCollection<Document> mockCollection = Mockito.mock(MongoCollection.class);
    MongoSinkBuilder.MongoSinkWriter writer =
        new MongoSinkBuilder.MongoSinkWriter("test-job", mockCollection);

    Metrics mockMetrics = mock(Metrics.class);
    var metricsField = writer.getClass().getDeclaredField("metrics");
    metricsField.setAccessible(true);
    metricsField.set(writer, mockMetrics);

    String payloadJson = JsonUtils.toJson(Map.of("id", 1, "name", "Charlie"));
    StreamEnvelop envelop =
        StreamEnvelop.builder()
            .operation("INSERT")
            .primaryKey("id")
            .payloadJson(payloadJson)
            .build();

    // when
    writer.write(envelop, null);

    // then
    verify(mockCollection, times(1))
        .replaceOne(any(Bson.class), any(Document.class), any(ReplaceOptions.class));
    verify(mockMetrics, times(1)).inc(MetricKeys.SINK_SUCCESS_COUNT);
    verify(mockMetrics, never()).inc(MetricKeys.SINK_ERROR_COUNT);
  }

  @SuppressWarnings("unchecked")
  @Test
  void testInvokeIncrementsErrorMetricOnFailure() throws Exception {
    // given
    MongoCollection<Document> mockCollection = Mockito.mock(MongoCollection.class);
    MongoSinkBuilder.MongoSinkWriter writer =
        new MongoSinkBuilder.MongoSinkWriter("test-job", mockCollection);

    Metrics mockMetrics = mock(Metrics.class);
    var metricsField = writer.getClass().getDeclaredField("metrics");
    metricsField.setAccessible(true);
    metricsField.set(writer, mockMetrics);

    doThrow(new RuntimeException("DB replace failed"))
        .when(mockCollection)
        .replaceOne(any(Bson.class), any(Document.class), any(ReplaceOptions.class));

    String payloadJson = JsonUtils.toJson(Map.of("id", 2, "name", "Alice"));
    StreamEnvelop envelop =
        StreamEnvelop.builder()
            .operation("INSERT")
            .primaryKey("id")
            .payloadJson(payloadJson)
            .build();

    // when
    writer.write(envelop, null);

    // then
    verify(mockCollection, times(1))
        .replaceOne(any(Bson.class), any(Document.class), any(ReplaceOptions.class));
    verify(mockMetrics, times(1)).inc(MetricKeys.SINK_ERROR_COUNT);
    verify(mockMetrics, never()).inc(MetricKeys.SINK_SUCCESS_COUNT);
  }
}
