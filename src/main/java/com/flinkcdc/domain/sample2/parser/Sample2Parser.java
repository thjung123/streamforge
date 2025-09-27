package com.flinkcdc.domain.sample2.parser;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flinkcdc.common.model.CdcEnvelop;
import com.flinkcdc.common.pipeline.PipelineBuilder;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;


public class Sample2Parser implements PipelineBuilder.ParserFunction<String, CdcEnvelop> {

    private static final Logger log = LoggerFactory.getLogger(Sample2Parser.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public DataStream<CdcEnvelop> parse(DataStream<String> input) {
        return input
                .map(json -> {
                    try {
                        return MAPPER.readValue(json, CdcEnvelop.class);
                    } catch (Exception e) {
                        log.warn("Failed JSON Parsing: {}", json, e);
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .name("Sample1Parser");
    }
}
