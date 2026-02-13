package com.streamforge.core.model;

import com.streamforge.core.util.JsonUtils;
import java.io.Serial;
import java.io.Serializable;
import java.time.Instant;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CdcEnvelop implements Serializable {

  @Serial private static final long serialVersionUID = 1L;

  private String operation;
  private String source;
  private String payloadJson;
  private Instant eventTime;
  private Instant processedTime;
  private String traceId;
  private String primaryKey;

  public static CdcEnvelop of(
      String operation, String source, Map<String, Object> payload, String primaryKey) {
    if (operation == null || source == null) {
      throw new IllegalArgumentException("operation and source must not be null");
    }

    return CdcEnvelop.builder()
        .operation(operation)
        .source(source)
        .payloadJson(payload != null ? JsonUtils.toJson(payload) : null)
        .primaryKey(primaryKey)
        .eventTime(Instant.now())
        .processedTime(Instant.now())
        .traceId(null)
        .build();
  }

  public static CdcEnvelop of(String operation, String source, Map<String, Object> payload) {
    return of(operation, source, payload, null);
  }

  public static CdcEnvelop fromJson(String json) {
    return JsonUtils.fromJson(json, CdcEnvelop.class);
  }

  public String toJson() {
    return JsonUtils.toJson(this);
  }

  @SuppressWarnings("unchecked")
  public Map<String, Object> getPayloadAsMap() {
    return payloadJson != null ? JsonUtils.fromJson(payloadJson, Map.class) : null;
  }
}
