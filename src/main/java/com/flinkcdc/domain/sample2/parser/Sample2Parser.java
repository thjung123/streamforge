package com.flinkcdc.domain.sample2.parser;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.flinkcdc.common.model.CdcEnvelop;
import com.flinkcdc.common.pipeline.PipelineBuilder;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;

public class Sample2Parser implements PipelineBuilder.ParserFunction<String, CdcEnvelop> {

    private static final Logger log = LoggerFactory.getLogger(Sample2Parser.class);
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    @Override
    public DataStream<CdcEnvelop> parse(DataStream<String> input) {
        return input
                .map(Sample2Parser::parseJson)
                .filter(Objects::nonNull)
                .name("Sample2Parser");
    }

    static CdcEnvelop parseJson(String json) {
        try {
            CdcEnvelop envelop = MAPPER.readValue(json, CdcEnvelop.class);

            return CdcEnvelop.builder()
                    .operation(envelop.getOperation())
                    .source(envelop.getSource())
                    .payload(envelop.getPayload())
                    .eventTime(envelop.getEventTime() != null ? envelop.getEventTime() : Instant.now())
                    .processedTime(Instant.now())
                    .traceId(envelop.getTraceId())
                    .primaryKeys(envelop.getPrimaryKeys())
                    .build();
        } catch (Exception e) {
            log.error("Failed to parse JSON: {}", json, e);
            return null;
        }
    }
}
