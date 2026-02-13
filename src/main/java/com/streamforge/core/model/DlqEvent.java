package com.streamforge.core.model;

import com.streamforge.core.util.JsonUtils;
import java.io.Serial;
import java.io.Serializable;
import java.time.Instant;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DlqEvent implements Serializable {

  @Serial private static final long serialVersionUID = 1L;

  private String errorType;
  private String errorMessage;
  private String source;
  private Instant timestamp;
  private String rawEvent;
  private String stacktrace;

  public static DlqEvent of(
      String errorType, String errorMessage, String source, String rawEvent, Throwable cause) {
    return DlqEvent.builder()
        .errorType(errorType)
        .errorMessage(errorMessage)
        .source(source)
        .timestamp(Instant.now())
        .rawEvent(rawEvent)
        .stacktrace(cause != null ? stacktraceToString(cause) : null)
        .build();
  }

  private static String stacktraceToString(Throwable t) {
    var sw = new java.io.StringWriter();
    t.printStackTrace(new java.io.PrintWriter(sw));
    return sw.toString();
  }

  public String toJson() {
    return JsonUtils.toJson(this);
  }
}
