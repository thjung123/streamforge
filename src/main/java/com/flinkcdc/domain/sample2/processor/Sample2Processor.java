package com.flinkcdc.domain.sample2.processor;

import com.flinkcdc.common.config.ErrorCodes;
import com.flinkcdc.common.config.MetricKeys;
import com.flinkcdc.common.dlq.DLQPublisher;
import com.flinkcdc.common.metric.Metrics;
import com.flinkcdc.common.model.CdcEnvelop;
import com.flinkcdc.common.model.DlqEvent;
import com.flinkcdc.common.pipeline.PipelineBuilder;
import com.flinkcdc.domain.sample2.Sample2Constants;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

public class Sample2Processor implements PipelineBuilder.ProcessorFunction<CdcEnvelop, CdcEnvelop> {

    private static final Logger log = LoggerFactory.getLogger(Sample2Processor.class);

    @Override
    public DataStream<CdcEnvelop> process(DataStream<CdcEnvelop> input) {
        return input
                .map(new RichMapFunction<CdcEnvelop, CdcEnvelop>() {

                    private transient Metrics metrics;

                    @Override
                    public void open(Configuration parameters) {
                        metrics = new Metrics(getRuntimeContext(), Sample2Constants.JOB_NAME, Sample2Constants.PROCESSOR_NAME);
                        DLQPublisher.getInstance().initMetrics(getRuntimeContext(), Sample2Constants.JOB_NAME);
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
                                    Sample2Constants.PROCESSOR_NAME,
                                    envelop != null ? envelop.toJson() : null,
                                    e
                            );
                            DLQPublisher.getInstance().publish(dlqEvent);
                            return null;
                        }
                    }
                })
                .filter(Objects::nonNull)
                .name(Sample2Constants.PROCESSOR_NAME);
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
