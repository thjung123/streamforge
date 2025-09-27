package com.flinkcdc.domain.sample2.processor;

import com.flinkcdc.common.model.CdcEnvelop;
import com.flinkcdc.common.pipeline.PipelineBuilder;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.time.Instant;
import java.util.UUID;


public class Sample2Processor implements PipelineBuilder.ProcessorFunction<CdcEnvelop, CdcEnvelop> {

    @Override
    public DataStream<CdcEnvelop> process(DataStream<CdcEnvelop> input) {
        return input
                .map(Sample2Processor::enrich)
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
