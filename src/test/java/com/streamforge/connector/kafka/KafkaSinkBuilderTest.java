package com.streamforge.connector.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.streamforge.core.config.MetricKeys;
import com.streamforge.core.dlq.DLQPublisher;
import com.streamforge.core.metric.Metrics;
import com.streamforge.core.model.StreamEnvelop;
import java.nio.charset.StandardCharsets;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class KafkaSinkBuilderTest {

  @Nested
  @DisplayName("MetricsMapFunction")
  class MetricsMapFunctionTest {

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
    @DisplayName("should increment success metric on successful map")
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
    @DisplayName("should increment error metric and publish to DLQ on failure")
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

  @Nested
  @DisplayName("DeliveryGuarantee configuration")
  class DeliveryGuaranteeTest {

    @Test
    @DisplayName("should default to AT_LEAST_ONCE when using no-arg constructor")
    void defaultGuarantee() {
      var builder = new KafkaSinkBuilder();

      assertThat(builder.getGuarantee()).isEqualTo(DeliveryGuarantee.AT_LEAST_ONCE);
      assertThat(builder.getTransactionalIdPrefix()).isNull();
    }

    @Test
    @DisplayName("should configure EXACTLY_ONCE via factory method")
    void exactlyOnceFactory() {
      var builder = KafkaSinkBuilder.exactlyOnce("txn-my-job");

      assertThat(builder.getGuarantee()).isEqualTo(DeliveryGuarantee.EXACTLY_ONCE);
      assertThat(builder.getTransactionalIdPrefix()).isEqualTo("txn-my-job");
    }

    @Test
    @DisplayName("should accept custom guarantee via constructor")
    void customGuarantee() {
      var builder = new KafkaSinkBuilder(DeliveryGuarantee.EXACTLY_ONCE, "txn-custom");

      assertThat(builder.getGuarantee()).isEqualTo(DeliveryGuarantee.EXACTLY_ONCE);
      assertThat(builder.getTransactionalIdPrefix()).isEqualTo("txn-custom");
    }
  }

  @Nested
  @DisplayName("Compaction configuration")
  class CompactionConfigTest {

    @Test
    @DisplayName("should default to compaction disabled")
    void defaultNoCompaction() {
      var builder = new KafkaSinkBuilder();

      assertThat(builder.isCompaction()).isFalse();
    }

    @Test
    @DisplayName("should enable compaction via factory method")
    void compactedFactory() {
      var builder = KafkaSinkBuilder.compacted();

      assertThat(builder.isCompaction()).isTrue();
      assertThat(builder.getGuarantee()).isEqualTo(DeliveryGuarantee.AT_LEAST_ONCE);
    }

    @Test
    @DisplayName("should enable compaction with EXACTLY_ONCE via factory method")
    void compactedExactlyOnceFactory() {
      var builder = KafkaSinkBuilder.compactedExactlyOnce("txn-compact");

      assertThat(builder.isCompaction()).isTrue();
      assertThat(builder.getGuarantee()).isEqualTo(DeliveryGuarantee.EXACTLY_ONCE);
      assertThat(builder.getTransactionalIdPrefix()).isEqualTo("txn-compact");
    }

    @Test
    @DisplayName("should not enable compaction for exactlyOnce factory")
    void exactlyOnceNoCompaction() {
      var builder = KafkaSinkBuilder.exactlyOnce("txn-job");

      assertThat(builder.isCompaction()).isFalse();
    }
  }

  @Nested
  @DisplayName("KeyedSerializationSchema")
  class KeyedSerializationSchemaTest {

    private static final String TOPIC = "test-topic";

    @Test
    @DisplayName("should always set key from primaryKey")
    void alwaysSetsKey() {
      var schema = new KafkaSinkBuilder.KeyedSerializationSchema(TOPIC, false);
      StreamEnvelop envelop =
          StreamEnvelop.builder()
              .operation("INSERT")
              .primaryKey("user-123")
              .payloadJson("{\"name\":\"Alice\"}")
              .build();

      ProducerRecord<byte[], byte[]> record = schema.serialize(envelop, null, 1000L);

      assertThat(record.topic()).isEqualTo(TOPIC);
      assertThat(record.key()).isEqualTo("user-123".getBytes(StandardCharsets.UTF_8));
      assertThat(record.value()).isNotNull();
    }

    @Test
    @DisplayName("should handle null primaryKey")
    void nullPrimaryKey() {
      var schema = new KafkaSinkBuilder.KeyedSerializationSchema(TOPIC, false);
      StreamEnvelop envelop =
          StreamEnvelop.builder().operation("INSERT").payloadJson("{\"id\":1}").build();

      ProducerRecord<byte[], byte[]> record = schema.serialize(envelop, null, 1000L);

      assertThat(record.key()).isNull();
      assertThat(record.value()).isNotNull();
    }

    @Test
    @DisplayName("should set timestamp on produced record")
    void setsTimestamp() {
      var schema = new KafkaSinkBuilder.KeyedSerializationSchema(TOPIC, false);
      StreamEnvelop envelop =
          StreamEnvelop.builder().operation("UPDATE").primaryKey("key-1").payloadJson("{}").build();

      ProducerRecord<byte[], byte[]> record = schema.serialize(envelop, null, 5000L);

      assertThat(record.timestamp()).isEqualTo(5000L);
    }

    @Test
    @DisplayName("should send tombstone (null value) on DELETE when compaction enabled")
    void tombstoneOnDeleteWithCompaction() {
      var schema = new KafkaSinkBuilder.KeyedSerializationSchema(TOPIC, true);
      StreamEnvelop envelop =
          StreamEnvelop.builder()
              .operation("DELETE")
              .primaryKey("user-123")
              .payloadJson("{\"name\":\"Alice\"}")
              .build();

      ProducerRecord<byte[], byte[]> record = schema.serialize(envelop, null, 1000L);

      assertThat(record.key()).isEqualTo("user-123".getBytes(StandardCharsets.UTF_8));
      assertThat(record.value()).isNull();
    }

    @Test
    @DisplayName("should handle case-insensitive DELETE with compaction")
    void caseInsensitiveDeleteWithCompaction() {
      var schema = new KafkaSinkBuilder.KeyedSerializationSchema(TOPIC, true);
      StreamEnvelop envelop =
          StreamEnvelop.builder()
              .operation("delete")
              .primaryKey("user-456")
              .payloadJson("{}")
              .build();

      ProducerRecord<byte[], byte[]> record = schema.serialize(envelop, null, 2000L);

      assertThat(record.key()).isEqualTo("user-456".getBytes(StandardCharsets.UTF_8));
      assertThat(record.value()).isNull();
    }

    @Test
    @DisplayName("should NOT send tombstone on DELETE when compaction disabled")
    void noTombstoneWithoutCompaction() {
      var schema = new KafkaSinkBuilder.KeyedSerializationSchema(TOPIC, false);
      StreamEnvelop envelop =
          StreamEnvelop.builder()
              .operation("DELETE")
              .primaryKey("user-123")
              .payloadJson("{\"name\":\"Alice\"}")
              .build();

      ProducerRecord<byte[], byte[]> record = schema.serialize(envelop, null, 1000L);

      assertThat(record.key()).isEqualTo("user-123".getBytes(StandardCharsets.UTF_8));
      assertThat(record.value()).isNotNull();
    }
  }
}
