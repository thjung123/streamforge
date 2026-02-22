package com.streamforge.pattern.materialization;

import java.io.Serializable;
import java.time.Instant;

public record ChangelogEvent<T>(ChangeType type, String key, T before, T after, Instant timestamp)
    implements Serializable {

  public enum ChangeType {
    INSERT,
    UPDATE,
    DELETE
  }
}
