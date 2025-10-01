package com.flinkcdc.common.metric;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class Metrics {

    private final MetricGroup metricGroup;
    private final ConcurrentMap<String, Counter> counters = new ConcurrentHashMap<>();

    public Metrics(RuntimeContext ctx, String jobName, String operatorName) {
        this.metricGroup = ctx.getMetricGroup()
                .addGroup("job", jobName)
                .addGroup("operator", operatorName);
    }

    public void inc(String name) {
        counters.computeIfAbsent(name, metricGroup::counter).inc();
    }
}

