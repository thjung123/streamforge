package com.flinkcdc.common.model;

import com.flinkcdc.common.utils.JsonUtils;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serial;
import java.io.Serializable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CdcEnvelop implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private String operation;
    private String source;
    private Map<String, Object> payload;
    private Instant eventTime;
    private Instant processedTime;
    private String traceId;
    private List<String> primaryKeys;

    public static CdcEnvelop of(String operation, String source, Map<String, Object> payload, List<String> primaryKeys) {
        if (operation == null || source == null) {
            throw new IllegalArgumentException("operation and source must not be null");
        }
        return CdcEnvelop.builder()
                .operation(operation)
                .source(source)
                .payload(payload == null ? null : new LinkedHashMap<>(payload))
                .primaryKeys(primaryKeys == null ? null : new ArrayList<>(primaryKeys))
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
}
