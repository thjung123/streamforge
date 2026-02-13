package com.streamforge.job.cdcsync.parser;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.streamforge.core.config.ErrorCodes;
import com.streamforge.core.config.MetricKeys;
import com.streamforge.core.dlq.DLQPublisher;
import com.streamforge.core.metric.Metrics;
import com.streamforge.core.model.CdcEnvelop;
import com.streamforge.core.model.DlqEvent;
import com.streamforge.core.pipeline.PipelineBuilder;
import com.streamforge.job.cdcsync.KafkaToMongoJob;
import java.time.Instant;
import java.util.Objects;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaToMongoParser implements PipelineBuilder.ParserFunction<String, CdcEnvelop> {

  private static final Logger log = LoggerFactory.getLogger(KafkaToMongoParser.class);
  private static final ObjectMapper MAPPER =
      new ObjectMapper()
          .registerModule(new JavaTimeModule())
          .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
          .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

  @Override
  public DataStream<CdcEnvelop> parse(DataStream<String> input) {
    return input
        .filter(json -> json != null && json.trim().startsWith("{") && json.trim().endsWith("}"))
        .map(
            new RichMapFunction<String, CdcEnvelop>() {
              private transient Metrics metrics;

              @Override
              public void open(Configuration parameters) {
                metrics =
                    new Metrics(
                        getRuntimeContext(), KafkaToMongoJob.JOB_NAME, KafkaToMongoJob.PARSER_NAME);
                DLQPublisher.getInstance()
                    .initMetrics(getRuntimeContext(), KafkaToMongoJob.JOB_NAME);
              }

              @Override
              public CdcEnvelop map(String json) {
                try {
                  CdcEnvelop env = parseJson(json);
                  metrics.inc(MetricKeys.PARSER_SUCCESS_COUNT);
                  return env;
                } catch (Exception e) {
                  metrics.inc(MetricKeys.PARSER_ERROR_COUNT);
                  log.warn("Failed to parse JSON: {}", json, e);
                  DlqEvent dlqEvent =
                      DlqEvent.of(
                          ErrorCodes.PARSING_ERROR,
                          e.getMessage(),
                          KafkaToMongoJob.PARSER_NAME,
                          json,
                          e);
                  DLQPublisher.getInstance().publish(dlqEvent);
                  return null;
                }
              }
            })
        .filter(Objects::nonNull)
        .name(KafkaToMongoJob.PARSER_NAME);
  }

  static CdcEnvelop parseJson(String json) throws Exception {
    CdcEnvelop envelop = MAPPER.readValue(json, CdcEnvelop.class);

    return CdcEnvelop.builder()
        .operation(envelop.getOperation())
        .source(envelop.getSource())
        .payloadJson(envelop.getPayloadJson())
        .eventTime(envelop.getEventTime() != null ? envelop.getEventTime() : Instant.now())
        .processedTime(Instant.now())
        .traceId(envelop.getTraceId())
        .primaryKey(envelop.getPrimaryKey())
        .build();
  }
}
