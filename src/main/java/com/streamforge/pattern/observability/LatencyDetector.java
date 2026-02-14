package com.streamforge.pattern.observability;

import com.streamforge.core.config.MetricKeys;
import com.streamforge.core.metric.Metrics;
import com.streamforge.core.pipeline.PipelineBuilder;
import java.time.Duration;
import java.time.Instant;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LatencyDetector<T> implements PipelineBuilder.StreamPattern<T> {

  public static final OutputTag<String> ALERT_TAG = new OutputTag<>("latency-alert") {};

  private final TimestampExtractor<T> eventTimeExtractor;
  private final Duration threshold;

  public LatencyDetector(TimestampExtractor<T> eventTimeExtractor, Duration threshold) {
    this.eventTimeExtractor = eventTimeExtractor;
    this.threshold = threshold;
  }

  @Override
  public DataStream<T> apply(DataStream<T> stream) {
    return stream
        .process(new LatencyFunction<>(eventTimeExtractor, threshold, name()))
        .name(name());
  }

  static class LatencyFunction<T> extends ProcessFunction<T, T> {

    private static final Logger log = LoggerFactory.getLogger(LatencyDetector.class);

    private final TimestampExtractor<T> eventTimeExtractor;
    private final Duration threshold;
    private final String operatorName;
    private transient Metrics metrics;
    private transient volatile long lastLatencyMs;

    LatencyFunction(
        TimestampExtractor<T> eventTimeExtractor, Duration threshold, String operatorName) {
      this.eventTimeExtractor = eventTimeExtractor;
      this.threshold = threshold;
      this.operatorName = operatorName;
    }

    @Override
    public void open(Configuration parameters) {
      this.metrics = new Metrics(getRuntimeContext(), "latency", operatorName);
      getRuntimeContext()
          .getMetricGroup()
          .addGroup("scope", "latency")
          .addGroup("operator", operatorName)
          .gauge("e2e_latency_ms", () -> lastLatencyMs);
    }

    @Override
    public void processElement(T value, Context ctx, Collector<T> out) {
      Instant eventTime = eventTimeExtractor.extract(value);
      if (eventTime != null) {
        long latencyMs = System.currentTimeMillis() - eventTime.toEpochMilli();
        lastLatencyMs = latencyMs;

        if (latencyMs > threshold.toMillis()) {
          metrics.inc(MetricKeys.LATENCY_ALERT_COUNT);
          log.warn(
              "[LatencyDetector] Latency {}ms exceeded threshold {}ms",
              latencyMs,
              threshold.toMillis());
          ctx.output(
              ALERT_TAG,
              "Latency " + latencyMs + "ms exceeded threshold " + threshold.toMillis() + "ms");
        }
      }
      out.collect(value);
    }
  }
}
