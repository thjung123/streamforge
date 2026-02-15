package com.streamforge.pattern.observability;

import com.streamforge.pattern.filter.SerializablePredicate;
import java.io.Serializable;

public class QualityCheck<T> implements Serializable {

  private final String name;
  private final SerializablePredicate<T> condition;

  private QualityCheck(String name, SerializablePredicate<T> condition) {
    this.name = name;
    this.condition = condition;
  }

  public static <T> QualityCheck<T> of(String name, SerializablePredicate<T> condition) {
    return new QualityCheck<>(name, condition);
  }

  public String name() {
    return name;
  }

  public boolean test(T value) {
    return condition.test(value);
  }
}
