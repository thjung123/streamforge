package com.streamforge.core.launcher;

import org.slf4j.LoggerFactory;

public interface StreamJob {
  String name();

  void run(String[] args) throws Exception;

  default void logCompletion(long netRuntimeMillis) {
    LoggerFactory.getLogger(getClass()).info("Job {} finished in {} ms", name(), netRuntimeMillis);
  }
}
