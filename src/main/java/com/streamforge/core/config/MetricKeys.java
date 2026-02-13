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

    public static final String KAFKA = "kafka";
    public static final String MONGO = "mongo";

}
