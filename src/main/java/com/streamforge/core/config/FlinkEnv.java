package com.streamforge.core.config;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public final class FlinkEnv {

  public static final String METRICS_PORT = "METRICS_PORT";

  private FlinkEnv() {}

  public static StreamExecutionEnvironment create() {
    String port = ScopedConfig.getGlobalOrDefault(METRICS_PORT, "");
    if (port.isBlank()) {
      return StreamExecutionEnvironment.getExecutionEnvironment();
    }
    Configuration conf = new Configuration();
    conf.setString(
        "metrics.reporter.prom.factory.class",
        "org.apache.flink.metrics.prometheus.PrometheusReporterFactory");
    conf.setString("metrics.reporter.prom.port", port);
    return StreamExecutionEnvironment.getExecutionEnvironment(conf);
  }
}
