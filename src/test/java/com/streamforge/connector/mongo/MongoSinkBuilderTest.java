package com.streamforge.connector.mongo;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.*;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.BulkWriteOptions;
import com.streamforge.core.config.MetricKeys;
import com.streamforge.core.dlq.DLQPublisher;
import com.streamforge.core.metric.Metrics;
import com.streamforge.core.model.StreamEnvelop;
import com.streamforge.core.util.JsonUtils;
import java.lang.reflect.Field;
import java.util.Map;
import org.bson.Document;
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
  void bufferedWritesAreFlushedAsABulkWrite() throws Exception {
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
    verify(mockCollection, never()).bulkWrite(anyList(), any(BulkWriteOptions.class)); // buffered
    writer.flush(true);

    // then
    verify(mockCollection, times(1)).bulkWrite(anyList(), any(BulkWriteOptions.class));
    verify(mockMetrics, times(1)).inc(MetricKeys.SINK_SUCCESS_COUNT);
    verify(mockMetrics, never()).inc(MetricKeys.SINK_ERROR_COUNT);
  }

  @SuppressWarnings("unchecked")
  @Test
  void bulkWriteFailureRoutesToDlqAndIncrementsErrorMetric() throws Exception {
    // given
    MongoCollection<Document> mockCollection = Mockito.mock(MongoCollection.class);
    MongoSinkBuilder.MongoSinkWriter writer =
        new MongoSinkBuilder.MongoSinkWriter("test-job", mockCollection);

    Metrics mockMetrics = mock(Metrics.class);
    var metricsField = writer.getClass().getDeclaredField("metrics");
    metricsField.setAccessible(true);
    metricsField.set(writer, mockMetrics);

    doThrow(new RuntimeException("bulk write failed"))
        .when(mockCollection)
        .bulkWrite(anyList(), any(BulkWriteOptions.class));

    String payloadJson = JsonUtils.toJson(Map.of("id", 2, "name", "Alice"));
    StreamEnvelop envelop =
        StreamEnvelop.builder()
            .operation("INSERT")
            .primaryKey("id")
            .payloadJson(payloadJson)
            .build();

    // when
    writer.write(envelop, null);
    writer.flush(true);

    // then
    verify(mockCollection, times(1)).bulkWrite(anyList(), any(BulkWriteOptions.class));
    verify(mockMetrics, times(1)).inc(MetricKeys.SINK_ERROR_COUNT);
    verify(mockMetrics, never()).inc(MetricKeys.SINK_SUCCESS_COUNT);
  }

  @SuppressWarnings("unchecked")
  @Test
  void poisonRecordIsRoutedToDlqNotCrashing() throws Exception {
    MongoCollection<Document> mockCollection = Mockito.mock(MongoCollection.class);
    MongoSinkBuilder.MongoSinkWriter writer =
        new MongoSinkBuilder.MongoSinkWriter("test-job", mockCollection);

    Metrics mockMetrics = mock(Metrics.class);
    var metricsField = writer.getClass().getDeclaredField("metrics");
    metricsField.setAccessible(true);
    metricsField.set(writer, mockMetrics);

    StreamEnvelop poison =
        StreamEnvelop.builder()
            .operation("INSERT")
            .primaryKey("id")
            .payloadJson("{ not valid json")
            .build();

    writer.write(poison, null); // must not throw
    writer.flush(true);

    verify(mockMetrics, times(1)).inc(MetricKeys.SINK_ERROR_COUNT);
    verify(mockMetrics, never()).inc(MetricKeys.SINK_SUCCESS_COUNT);
    verify(mockCollection, never()).bulkWrite(anyList(), any(BulkWriteOptions.class));
  }
}
