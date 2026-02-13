package com.streamforge.job.cdcsync.processor;

import com.streamforge.core.model.CdcEnvelop;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class MongoToKafkaProcessorTest {

    @Test
    void enrich_shouldSetProcessedTimeAndGenerateTraceId_whenTraceIdIsNull() {
        // given
        Map<String, Object> payload = new HashMap<>();
        payload.put("id", 42);

        CdcEnvelop envelop = CdcEnvelop.of("update", "orders", payload);
        envelop.setTraceId(null);
        envelop.setProcessedTime(null);

        // when
        CdcEnvelop result = invokeEnrich(envelop);

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

        CdcEnvelop envelop = CdcEnvelop.of("update", "orders", payload);
        envelop.setTraceId("trace-12345");
        envelop.setProcessedTime(null);

        // when
        CdcEnvelop result = invokeEnrich(envelop);

        // then
        assertThat(result.getTraceId()).isEqualTo("trace-12345");
        assertThat(result.getProcessedTime()).isNotNull();
    }

    @Test
    void enrich_shouldReturnNull_whenInputIsNull() {
        // when
        CdcEnvelop result = invokeEnrich(null);

        // then
        assertThat(result).isNull();
    }

    private CdcEnvelop invokeEnrich(CdcEnvelop envelop) {
        try {
            var method = MongoToKafkaProcessor.class.getDeclaredMethod("enrich", CdcEnvelop.class);
            method.setAccessible(true);
            return (CdcEnvelop) method.invoke(null, envelop);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
