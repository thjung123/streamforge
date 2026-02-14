package com.streamforge.core.metric;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;

public class Metrics {

  private final MetricGroup metricGroup;
  private final ConcurrentMap<String, Counter> counters = new ConcurrentHashMap<>();

  public Metrics(RuntimeContext ctx, String scope, String operatorName) {
    this(ctx.getMetricGroup(), scope, operatorName);
  }

  public Metrics(MetricGroup baseGroup, String scope, String operatorName) {
    this.metricGroup = baseGroup.addGroup("scope", scope).addGroup("operator", operatorName);
  }

  public void inc(String name) {
    counters.computeIfAbsent(name, metricGroup::counter).inc();
  }
}
