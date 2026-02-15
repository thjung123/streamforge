package com.streamforge.pattern.observability;

import java.io.Serializable;
import java.util.Map;

@FunctionalInterface
public interface MetadataAccessor<T> extends Serializable {
  Map<String, String> getMetadata(T value);
}
