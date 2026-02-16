package com.streamforge.connector.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.streamforge.core.config.MetricKeys;
import com.streamforge.core.dlq.DLQPublisher;
import com.streamforge.core.metric.Metrics;
import com.streamforge.core.model.StreamEnvelop;
import org.apache.flink.connector.base.DeliveryGuarantee;
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
}
