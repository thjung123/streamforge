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

  public static final String OBSERVER_TOTAL_COUNT = "observer.total_events";

  public static final String LATE_EVENT_COUNT = "late.event_count";

  public static final String DECORATOR_EVENT_COUNT = "decorator.event_count";

  public static final String MERGER_PASS_COUNT = "merger.pass_count";
  public static final String MERGER_SUPPRESSED_COUNT = "merger.suppressed_count";

  public static final String SCHEMA_PASS_COUNT = "schema.pass_count";
  public static final String SCHEMA_VIOLATION_COUNT = "schema.violation_count";

  public static final String SPLITTER_UNMATCHED_COUNT = "splitter.unmatched_count";

  public static final String FANIN_MERGED_COUNT = "fanin.merged_count";

  public static final String JOINER_MATCH_COUNT = "joiner.match_count";
  public static final String JOINER_MISS_COUNT = "joiner.miss_count";
  public static final String JOINER_REF_UPDATE_COUNT = "joiner.ref_update_count";

  public static final String DYNAMIC_JOINER_MATCH_COUNT = "dynamic_joiner.match_count";
  public static final String DYNAMIC_JOINER_LEFT_PASS_COUNT = "dynamic_joiner.left_pass_count";
  public static final String DYNAMIC_JOINER_RIGHT_PASS_COUNT = "dynamic_joiner.right_pass_count";
  public static final String DYNAMIC_JOINER_EXPIRE_COUNT = "dynamic_joiner.expire_count";

  public static final String KAFKA = "kafka";
  public static final String MONGO = "mongo";
}
