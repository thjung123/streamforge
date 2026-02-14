package com.streamforge.pattern.quality.rules;

import com.streamforge.core.model.StreamEnvelop;
import com.streamforge.pattern.quality.ConstraintRule;
import java.util.Map;
import java.util.regex.Pattern;

public class FormatRule implements ConstraintRule<StreamEnvelop> {

  private final String fieldName;
  private final Pattern pattern;

  public FormatRule(String fieldName, String regex) {
    this.fieldName = fieldName;
    this.pattern = Pattern.compile(regex);
  }

  public static FormatRule email(String fieldName) {
    return new FormatRule(fieldName, "^[\\w.-]+@[\\w.-]+\\.[a-zA-Z]{2,}$");
  }

  public static FormatRule date(String fieldName) {
    return new FormatRule(fieldName, "^\\d{4}-\\d{2}-\\d{2}$");
  }

  @Override
  public String validate(StreamEnvelop value) {
    Map<String, Object> payload = value.getPayloadAsMap();
    if (payload == null || !payload.containsKey(fieldName)) {
      return null;
    }
    Object raw = payload.get(fieldName);
    if (raw == null) {
      return null;
    }
    String str = raw.toString();
    if (!pattern.matcher(str).matches()) {
      return fieldName + " does not match format: " + pattern.pattern();
    }
    return null;
  }
}
