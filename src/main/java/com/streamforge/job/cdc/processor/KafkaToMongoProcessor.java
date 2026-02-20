package com.streamforge.job.cdc.processor;

import com.streamforge.core.config.ErrorCodes;
import com.streamforge.core.config.MetricKeys;
import com.streamforge.core.dlq.DLQPublisher;
import com.streamforge.core.metric.Metrics;
import com.streamforge.core.model.DlqEvent;
import com.streamforge.core.model.StreamEnvelop;
import com.streamforge.core.pipeline.PipelineBuilder;
import com.streamforge.job.cdc.KafkaToMongoJob;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaToMongoProcessor
    implements PipelineBuilder.ProcessorFunction<StreamEnvelop, StreamEnvelop> {

  private static final Logger log = LoggerFactory.getLogger(KafkaToMongoProcessor.class);

  @Override
  public DataStream<StreamEnvelop> process(DataStream<StreamEnvelop> input) {
    return input
        .map(
            new RichMapFunction<StreamEnvelop, StreamEnvelop>() {

              private transient Metrics metrics;

              @Override
              public void open(Configuration parameters) {
                metrics =
                    new Metrics(
                        getRuntimeContext(),
                        KafkaToMongoJob.JOB_NAME,
                        KafkaToMongoJob.PROCESSOR_NAME);
                DLQPublisher.getInstance()
                    .initMetrics(getRuntimeContext(), KafkaToMongoJob.JOB_NAME);
              }

              @Override
              public StreamEnvelop map(StreamEnvelop envelop) {
                try {
                  StreamEnvelop result = enrich(envelop);
                  metrics.inc(MetricKeys.PROCESSOR_SUCCESS_COUNT);
                  return result;
                } catch (Exception e) {
                  metrics.inc(MetricKeys.PROCESSOR_ERROR_COUNT);
                  log.error("Failed to process StreamEnvelop: {}", envelop, e);

                  DlqEvent dlqEvent =
                      DlqEvent.of(
                          ErrorCodes.PROCESSING_ERROR,
                          e.getMessage(),
                          KafkaToMongoJob.PROCESSOR_NAME,
                          envelop != null ? envelop.toJson() : null,
                          e);
                  DLQPublisher.getInstance().publish(dlqEvent);
                  return null;
                }
              }
            })
        .filter(Objects::nonNull)
        .name(KafkaToMongoJob.PROCESSOR_NAME);
  }

  private static StreamEnvelop enrich(StreamEnvelop envelop) {
    if (envelop == null) {
      throw new IllegalArgumentException("Envelop cannot be null during processing");
    }

    envelop.setProcessedTime(Instant.now());

    if (envelop.getTraceId() == null || envelop.getTraceId().isEmpty()) {
      envelop.setTraceId("trace-" + UUID.randomUUID());
    }

    return envelop;
  }
}
