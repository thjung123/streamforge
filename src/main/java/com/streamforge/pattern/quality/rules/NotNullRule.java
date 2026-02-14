package com.streamforge.pattern.quality.rules;

import com.streamforge.core.model.StreamEnvelop;
import com.streamforge.pattern.quality.ConstraintRule;
import java.util.Map;

public class NotNullRule implements ConstraintRule<StreamEnvelop> {

  private final String fieldName;

  public NotNullRule(String fieldName) {
    this.fieldName = fieldName;
  }

  @Override
  public String validate(StreamEnvelop value) {
    Map<String, Object> payload = value.getPayloadAsMap();
    if (payload == null || !payload.containsKey(fieldName) || payload.get(fieldName) == null) {
      return fieldName + " must not be null";
    }
    return null;
  }
}
