package com.streamforge.pattern.quality.rules;

import com.streamforge.core.model.StreamEnvelop;
import com.streamforge.pattern.quality.ConstraintRule;
import java.util.Map;

public class RangeRule implements ConstraintRule<StreamEnvelop> {

  private final String fieldName;
  private final double min;
  private final double max;

  public RangeRule(String fieldName, double min, double max) {
    this.fieldName = fieldName;
    this.min = min;
    this.max = max;
  }

  @Override
  public String validate(StreamEnvelop value) {
    Map<String, Object> payload = value.getPayloadAsMap();
    if (payload == null || !payload.containsKey(fieldName)) {
      return null;
    }
    Object raw = payload.get(fieldName);
    if (!(raw instanceof Number)) {
      return fieldName + " is not a number";
    }
    double val = ((Number) raw).doubleValue();
    if (val < min || val > max) {
      return fieldName + " must be between " + min + " and " + max;
    }
    return null;
  }
}
