package com.flinkcdc.domain.kafka_to_mongo.processor;

import com.flinkcdc.common.model.CdcEnvelop;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

class Sample2ProcessorUnitTest {

    @Test
    void enrich_shouldSetProcessedTimeAndGenerateTraceId_whenTraceIdIsNull() {
        // given
        Map<String, Object> payload = new HashMap<>();
        payload.put("id", 42);

        CdcEnvelop envelop = CdcEnvelop.of("insert", "orders", payload);
        envelop.setTraceId(null);
        envelop.setProcessedTime(null);

        // when
        CdcEnvelop result = invokeEnrich(envelop);

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

        CdcEnvelop envelop = CdcEnvelop.of("insert", "orders", payload);
        envelop.setTraceId("trace-abc-999");
        envelop.setProcessedTime(null);

        // when
        CdcEnvelop result = invokeEnrich(envelop);

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


    private CdcEnvelop invokeEnrich(CdcEnvelop envelop) {
        try {
            var method = KafkaToMongoProcessor.class.getDeclaredMethod("enrich", CdcEnvelop.class);
            method.setAccessible(true);
            return (CdcEnvelop) method.invoke(null, envelop);
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
