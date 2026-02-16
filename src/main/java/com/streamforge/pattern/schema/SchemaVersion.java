package com.streamforge.pattern.schema;

import java.io.Serializable;
import java.util.*;

public class SchemaVersion implements Serializable {

  private final Map<String, FieldType> requiredFields;
  private final Set<String> optionalFields;
  private final boolean strict;

  private SchemaVersion(
      Map<String, FieldType> requiredFields, Set<String> optionalFields, boolean strict) {
    this.requiredFields = new LinkedHashMap<>(requiredFields);
    this.optionalFields = new LinkedHashSet<>(optionalFields);
    this.strict = strict;
  }

  public List<String> validate(Map<String, Object> payload) {
    if (payload == null) return List.of("payload is null");

    List<String> violations = new ArrayList<>();

    for (Map.Entry<String, FieldType> entry : requiredFields.entrySet()) {
      String field = entry.getKey();
      FieldType expectedType = entry.getValue();
      Object value = payload.get(field);

      if (value == null) {
        violations.add("missing required field: " + field);
      } else if (!expectedType.matches(value)) {
        violations.add(
            field + " expected " + expectedType + " but got " + value.getClass().getSimpleName());
      }
    }

    if (strict) {
      for (String field : payload.keySet()) {
        if (!requiredFields.containsKey(field) && !optionalFields.contains(field)) {
          violations.add("unknown field: " + field);
        }
      }
    }

    return violations;
  }

  public static Builder builder() {
    return new Builder();
  }

  public enum FieldType {
    STRING(String.class),
    NUMBER(Number.class),
    BOOLEAN(Boolean.class),
    MAP(Map.class),
    LIST(List.class),
    ANY(Object.class);

    private final Class<?> javaType;

    FieldType(Class<?> javaType) {
      this.javaType = javaType;
    }

    public boolean matches(Object value) {
      return javaType.isInstance(value);
    }
  }

  public static class Builder {
    private final Map<String, FieldType> requiredFields = new LinkedHashMap<>();
    private final Set<String> optionalFields = new LinkedHashSet<>();
    private boolean strict = false;

    private Builder() {}

    public Builder required(String field, FieldType type) {
      requiredFields.put(field, type);
      return this;
    }

    public Builder optional(String field) {
      optionalFields.add(field);
      return this;
    }

    public Builder strict() {
      this.strict = true;
      return this;
    }

    public SchemaVersion build() {
      return new SchemaVersion(requiredFields, optionalFields, strict);
    }
  }
}
