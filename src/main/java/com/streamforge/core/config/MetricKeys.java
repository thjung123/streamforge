package com.streamforge.core.config;

public final class MetricKeys {

  private MetricKeys() {}

  public static final String PARSER_SUCCESS_COUNT = "parser.success_count";
  public static final String PARSER_ERROR_COUNT = "parser.error_count";

  public static final String PROCESSOR_SUCCESS_COUNT = "processor.success_count";
  public static final String PROCESSOR_ERROR_COUNT = "processor.error_count";

  public static final String SINK_SUCCESS_COUNT = "sink.success_count";
  public static final String SINK_ERROR_COUNT = "sink.error_count";

  public static final String DLQ_PUBLISHED_COUNT = "dlq.published_count";
  public static final String DLQ_FAILED_COUNT = "dlq.failed_count";

  public static final String SOURCE_READ_COUNT = "source.read_count";
  public static final String SOURCE_ERROR_COUNT = "source.error_count";

  public static final String FILTER_PASS_COUNT = "filter.pass_count";
  public static final String FILTER_DROP_COUNT = "filter.drop_count";

  public static final String CONSTRAINT_PASS_COUNT = "constraint.pass_count";
  public static final String CONSTRAINT_VIOLATION_COUNT = "constraint.violation_count";

  public static final String DEDUP_PASS_COUNT = "dedup.pass_count";
  public static final String DEDUP_DUPLICATE_COUNT = "dedup.duplicate_count";

  public static final String LATENCY_ALERT_COUNT = "latency.alert_count";

  public static final String FLOW_DISRUPTION_COUNT = "flow.disruption_count";
  public static final String FLOW_RECOVERY_COUNT = "flow.recovery_count";

  public static final String KAFKA = "kafka";
  public static final String MONGO = "mongo";
}
