package com.streamforge.core.config;

import java.time.Duration;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public final class CheckpointConfig {

  private CheckpointConfig() {}

  public static void enableExactlyOnce(StreamExecutionEnvironment env) {
    configure(env, CheckpointingMode.EXACTLY_ONCE, Duration.ofMinutes(1));
  }

  public static void configure(
      StreamExecutionEnvironment env, CheckpointingMode mode, Duration interval) {
    var config = env.getCheckpointConfig();
    config.setCheckpointingConsistencyMode(mode);
    config.setCheckpointInterval(interval.toMillis());
    config.setMinPauseBetweenCheckpoints(interval.toMillis() / 2);
    config.setCheckpointTimeout(interval.toMillis() * 2);
    config.setMaxConcurrentCheckpoints(1);
  }
}
