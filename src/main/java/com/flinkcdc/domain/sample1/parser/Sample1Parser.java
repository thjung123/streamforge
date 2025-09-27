package com.flinkcdc.domain.sample1.parser;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flinkcdc.common.model.CdcEnvelop;
import com.flinkcdc.common.pipeline.PipelineBuilder;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

public class Sample1Parser implements PipelineBuilder.ParserFunction<Document, CdcEnvelop> {

    private static final Logger log = LoggerFactory.getLogger(Sample1Parser.class);
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true);

    @Override
    public DataStream<CdcEnvelop> parse(DataStream<Document> input) {
        return input
                .map(doc -> {
                    try {
                        return MAPPER.convertValue(doc, CdcEnvelop.class);
                    } catch (Exception e) {
                        log.warn("Failed to convert Mongo Document to CdcEnvelop: {}", doc, e);
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .name("Sample1Parser");
    }
}
