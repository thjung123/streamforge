package com.flinkcdc.common.model;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class CdcEnvelopTest {

    @Test
    @DisplayName("of() should create a CdcEnvelop with current timestamps and provided values")
    void testOfFactoryMethod() {
        Map<String, Object> payload = Map.of("id", 1, "name", "Alice");

        CdcEnvelop envelop = CdcEnvelop.of("CREATE", "users", payload);

        assertEquals("CREATE", envelop.getOperation());
        assertEquals("users", envelop.getSource());
        assertEquals(payload, envelop.getPayload());
        assertNotNull(envelop.getEventTime());
        assertNotNull(envelop.getProcessedTime());
        assertNull(envelop.getPrimaryKeys());

        assertTrue(Math.abs(envelop.getEventTime().getEpochSecond() - envelop.getProcessedTime().getEpochSecond()) <= 1);
    }


    @Test
    @DisplayName("toJson() and fromJson() should correctly serialize and deserialize the object")
    void testJsonSerialization() {

        CdcEnvelop original = CdcEnvelop.builder()
                .operation("UPDATE")
                .source("orders")
                .payload(new LinkedHashMap<>(Map.of("orderId", 1234, "status", "SHIPPED")))
                .primaryKeys(new ArrayList<>(List.of("orderId")))
                .eventTime(Instant.now())
                .processedTime(Instant.now())
                .traceId("trace-abc-123")
                .build();
        // Serialize to JSON
        String json = original.toJson();
        assertNotNull(json);
        assertTrue(json.contains("UPDATE"));
        assertTrue(json.contains("orders"));
        assertTrue(json.contains("trace-abc-123"));
        assertTrue(json.contains("orderId"));

        // Deserialize back
        CdcEnvelop restored = CdcEnvelop.fromJson(json);
        assertEquals(original.getOperation(), restored.getOperation());
        assertEquals(original.getSource(), restored.getSource());
        assertEquals(original.getPayload(), restored.getPayload());
        assertEquals(original.getTraceId(), restored.getTraceId());
        assertEquals(original.getPrimaryKeys(), restored.getPrimaryKeys());
    }

    @Test
    @DisplayName("equals() and hashCode() should work for objects with same field values")
    void testEqualsAndHashCode() {
        LinkedHashMap<String, Object> payload = new LinkedHashMap<>();
        payload.put("sku", "ABC123");

        CdcEnvelop e1 = CdcEnvelop.builder()
                .operation("DELETE")
                .source("inventory")
                .payload(payload)
                .primaryKeys(new ArrayList<>(List.of("sku")))
                .eventTime(Instant.now())
                .processedTime(Instant.now())
                .traceId("trace-1")
                .build();

        CdcEnvelop e2 = CdcEnvelop.builder()
                .operation("DELETE")
                .source("inventory")
                .payload(payload)
                .primaryKeys(new ArrayList<>(List.of("sku")))
                .eventTime(e1.getEventTime())
                .processedTime(e1.getProcessedTime())
                .traceId("trace-1")
                .build();

        assertEquals(e1, e2);
        assertEquals(e1.hashCode(), e2.hashCode());
    }
}
