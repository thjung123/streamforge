package com.flinkcdc.domain.sample1.processor;

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


public class Sample1Processor implements PipelineBuilder.ProcessorFunction<CdcEnvelop, CdcEnvelop> {
    private static final Logger log = LoggerFactory.getLogger(Sample1Processor.class);

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
                                "sample1-processor",
                                envelop != null ? envelop.toJson() : null,
                                e
                        );
                        DLQPublisher.getInstance().publish(dlqEvent);

                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .name("Sample1Processor");
    }


    private static CdcEnvelop enrich(CdcEnvelop envelop) {
        if (envelop == null) {
            return null;
        }

        envelop.setProcessedTime(Instant.now());

        if (envelop.getTraceId() == null || envelop.getTraceId().isEmpty()) {
            envelop.setTraceId("trace-" + UUID.randomUUID());
        }

        return envelop;
    }
}
