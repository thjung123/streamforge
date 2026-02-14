package com.streamforge.pattern.observability;

import java.io.Serializable;
import java.time.Instant;

@FunctionalInterface
public interface TimestampExtractor<T> extends Serializable {
  Instant extract(T value);
}
