package com.streamforge.core.model;

import static org.junit.jupiter.api.Assertions.*;

import com.streamforge.core.util.JsonUtils;
import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class StreamEnvelopTest {

  @Test
  @DisplayName("of() should create a StreamEnvelop with current timestamps and provided values")
  void testOfFactoryMethod() {
    Map<String, Object> payload = Map.of("id", 1, "name", "Alice");

    StreamEnvelop envelop = StreamEnvelop.of("CREATE", "users", payload);

    assertEquals("CREATE", envelop.getOperation());
    assertEquals("users", envelop.getSource());
    assertEquals(payload, envelop.getPayloadAsMap());
    assertNotNull(envelop.getEventTime());
    assertNotNull(envelop.getProcessedTime());
    assertNull(envelop.getPrimaryKey());
    assertTrue(
        Math.abs(
                envelop.getEventTime().getEpochSecond()
                    - envelop.getProcessedTime().getEpochSecond())
            <= 1);
  }

  @Test
  @DisplayName("toJson() and fromJson() should correctly serialize and deserialize the object")
  void testJsonSerialization() {
    StreamEnvelop original =
        StreamEnvelop.builder()
            .operation("UPDATE")
            .source("orders")
            .payloadJson(JsonUtils.toJson(Map.of("orderId", 1234, "status", "SHIPPED")))
            .primaryKey("orderId")
            .eventTime(Instant.now())
            .processedTime(Instant.now())
            .traceId("trace-abc-123")
            .build();

    String json = original.toJson();
    assertNotNull(json);
    assertTrue(json.contains("UPDATE"));
    assertTrue(json.contains("orders"));
    assertTrue(json.contains("trace-abc-123"));
    assertTrue(json.contains("orderId"));

    StreamEnvelop restored = StreamEnvelop.fromJson(json);
    assertEquals(original.getOperation(), restored.getOperation());
    assertEquals(original.getSource(), restored.getSource());
    assertEquals(original.getPayloadAsMap(), restored.getPayloadAsMap());
    assertEquals(original.getTraceId(), restored.getTraceId());
    assertEquals(original.getPrimaryKey(), restored.getPrimaryKey());
  }

  @Test
  @DisplayName("equals() and hashCode() should work for objects with same field values")
  void testEqualsAndHashCode() {
    String payloadJson = JsonUtils.toJson(Map.of("sku", "ABC123"));

    StreamEnvelop e1 =
        StreamEnvelop.builder()
            .operation("DELETE")
            .source("inventory")
            .payloadJson(payloadJson)
            .primaryKey("sku")
            .eventTime(Instant.now())
            .processedTime(Instant.now())
            .traceId("trace-1")
            .build();

    StreamEnvelop e2 =
        StreamEnvelop.builder()
            .operation("DELETE")
            .source("inventory")
            .payloadJson(payloadJson)
            .primaryKey("sku")
            .eventTime(e1.getEventTime())
            .processedTime(e1.getProcessedTime())
            .traceId("trace-1")
            .build();

    assertEquals(e1, e2);
    assertEquals(e1.hashCode(), e2.hashCode());
  }
}
