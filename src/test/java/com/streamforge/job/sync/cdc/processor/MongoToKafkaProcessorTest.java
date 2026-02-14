package com.streamforge.job.sync.cdc.processor;

import static org.assertj.core.api.Assertions.assertThat;

import com.streamforge.core.model.StreamEnvelop;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class MongoToKafkaProcessorTest {

  @Test
  void enrich_shouldSetProcessedTimeAndGenerateTraceId_whenTraceIdIsNull() {
    // given
    Map<String, Object> payload = new HashMap<>();
    payload.put("id", 42);

    StreamEnvelop envelop = StreamEnvelop.of("update", "orders", payload);
    envelop.setTraceId(null);
    envelop.setProcessedTime(null);

    // when
    StreamEnvelop result = invokeEnrich(envelop);

    // then
    assertThat(result.getProcessedTime()).isNotNull();
    assertThat(result.getProcessedTime()).isAfterOrEqualTo(Instant.now().minusSeconds(2));
    assertThat(result.getTraceId()).isNotNull().startsWith("trace-");
  }

  @Test
  void enrich_shouldNotOverrideExistingTraceId() {
    // given
    Map<String, Object> payload = new HashMap<>();
    payload.put("id", 42);

    StreamEnvelop envelop = StreamEnvelop.of("update", "orders", payload);
    envelop.setTraceId("trace-12345");
    envelop.setProcessedTime(null);

    // when
    StreamEnvelop result = invokeEnrich(envelop);

    // then
    assertThat(result.getTraceId()).isEqualTo("trace-12345");
    assertThat(result.getProcessedTime()).isNotNull();
  }

  @Test
  void enrich_shouldReturnNull_whenInputIsNull() {
    // when
    StreamEnvelop result = invokeEnrich(null);

    // then
    assertThat(result).isNull();
  }

  private StreamEnvelop invokeEnrich(StreamEnvelop envelop) {
    try {
      var method = MongoToKafkaProcessor.class.getDeclaredMethod("enrich", StreamEnvelop.class);
      method.setAccessible(true);
      return (StreamEnvelop) method.invoke(null, envelop);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
