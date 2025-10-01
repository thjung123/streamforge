package com.flinkcdc.domain.sample2.processor;

import com.flinkcdc.common.dlq.DLQPublisher;
import com.flinkcdc.common.model.CdcEnvelop;
import com.flinkcdc.common.model.DlqEvent;
import com.flinkcdc.common.pipeline.PipelineBuilder;
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
                .map(envelop -> {
                    try {
                        return enrich(envelop);
                    } catch (Exception e) {
                        log.error("Failed to process CdcEnvelop: {}", envelop, e);
                        DlqEvent dlqEvent = DlqEvent.of(
                                "PROCESSING_ERROR",
                                e.getMessage(),
                                "sample2-processor",
                                envelop != null ? envelop.toJson() : null,
                                e
                        );
                        DLQPublisher.getInstance().publish(dlqEvent);
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .name("Sample2Processor");
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
