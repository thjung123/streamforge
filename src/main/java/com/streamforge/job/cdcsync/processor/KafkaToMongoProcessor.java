package com.streamforge.job.cdcsync.processor;

import com.streamforge.core.config.ErrorCodes;
import com.streamforge.core.config.MetricKeys;
import com.streamforge.core.dlq.DLQPublisher;
import com.streamforge.core.metric.Metrics;
import com.streamforge.core.model.CdcEnvelop;
import com.streamforge.core.model.DlqEvent;
import com.streamforge.core.pipeline.PipelineBuilder;
import com.streamforge.job.cdcsync.KafkaToMongoJob;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

public class KafkaToMongoProcessor implements PipelineBuilder.ProcessorFunction<CdcEnvelop, CdcEnvelop> {

    private static final Logger log = LoggerFactory.getLogger(KafkaToMongoProcessor.class);

    @Override
    public DataStream<CdcEnvelop> process(DataStream<CdcEnvelop> input) {
        return input
                .map(new RichMapFunction<CdcEnvelop, CdcEnvelop>() {

                    private transient Metrics metrics;

                    @Override
                    public void open(Configuration parameters) {
                        metrics = new Metrics(getRuntimeContext(), KafkaToMongoJob.JOB_NAME, KafkaToMongoJob.PROCESSOR_NAME);
                        DLQPublisher.getInstance().initMetrics(getRuntimeContext(), KafkaToMongoJob.JOB_NAME);
                    }

                    @Override
                    public CdcEnvelop map(CdcEnvelop envelop) {
                        try {
                            CdcEnvelop result = enrich(envelop);
                            metrics.inc(MetricKeys.PROCESSOR_SUCCESS_COUNT);
                            return result;
                        } catch (Exception e) {
                            metrics.inc(MetricKeys.PROCESSOR_ERROR_COUNT);
                            log.error("Failed to process CdcEnvelop: {}", envelop, e);

                            DlqEvent dlqEvent = DlqEvent.of(
                                    ErrorCodes.PROCESSING_ERROR,
                                    e.getMessage(),
                                    KafkaToMongoJob.PROCESSOR_NAME,
                                    envelop != null ? envelop.toJson() : null,
                                    e
                            );
                            DLQPublisher.getInstance().publish(dlqEvent);
                            return null;
                        }
                    }
                })
                .filter(Objects::nonNull)
                .name(KafkaToMongoJob.PROCESSOR_NAME);
    }

    private static CdcEnvelop enrich(CdcEnvelop envelop) {
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
