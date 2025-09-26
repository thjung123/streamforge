package com.flinkcdc.common.model;

import com.flinkcdc.common.utils.JsonUtils;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.time.Instant;
import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CdcEnvelop implements Serializable {

    private String operation;
    private String source;
    private Map<String, Object> payload;
    private Instant eventTime;
    private Instant processedTime;
    private String traceId;

    public static CdcEnvelop of(String operation, String source, Map<String, Object> payload) {
        return CdcEnvelop.builder()
                .operation(operation)
                .source(source)
                .payload(payload)
                .eventTime(Instant.now())
                .processedTime(Instant.now())
                .build();
    }

    public static CdcEnvelop fromJson(String json) {
        return JsonUtils.fromJson(json, CdcEnvelop.class);
    }

    public String toJson() {
        return JsonUtils.toJson(this);
    }
}
