package com.streamforge.job.sync.cdc.processor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

import com.streamforge.core.model.StreamEnvelop;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class KafkaToMongoProcessorTest {

  @Test
  void enrich_shouldSetProcessedTimeAndGenerateTraceId_whenTraceIdIsNull() {
    // given
    Map<String, Object> payload = new HashMap<>();
    payload.put("id", 42);

    StreamEnvelop envelop = StreamEnvelop.of("insert", "orders", payload);
    envelop.setTraceId(null);
    envelop.setProcessedTime(null);

    // when
    StreamEnvelop result = invokeEnrich(envelop);

    // then
    assertThat(result).isNotNull();
    assertThat(result.getProcessedTime()).isNotNull();
    assertThat(result.getProcessedTime()).isAfterOrEqualTo(Instant.now().minusSeconds(2));
    assertThat(result.getTraceId()).isNotNull().startsWith("trace-");
  }

  @Test
  void enrich_shouldNotOverrideExistingTraceId() {
    // given
    Map<String, Object> payload = new HashMap<>();
    payload.put("id", 42);

    StreamEnvelop envelop = StreamEnvelop.of("insert", "orders", payload);
    envelop.setTraceId("trace-abc-999");
    envelop.setProcessedTime(null);

    // when
    StreamEnvelop result = invokeEnrich(envelop);

    // then
    assertThat(result).isNotNull();
    assertThat(result.getTraceId()).isEqualTo("trace-abc-999");
    assertThat(result.getProcessedTime()).isNotNull();
  }

  @Test
  void enrich_shouldThrowException_whenInputIsNull() {
    assertThatThrownBy(() -> invokeEnrich(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Envelop cannot be null");
  }

  private StreamEnvelop invokeEnrich(StreamEnvelop envelop) {
    try {
      var method = KafkaToMongoProcessor.class.getDeclaredMethod("enrich", StreamEnvelop.class);
      method.setAccessible(true);
      return (StreamEnvelop) method.invoke(null, envelop);
    } catch (Exception e) {
      if (e.getCause() != null) {
        if (e.getCause() instanceof RuntimeException re) {
          throw re;
        }
        throw new RuntimeException(e.getCause());
      }
      throw new RuntimeException(e);
    }
  }
}
