package com.flinkcdc.domain.sample1.parser;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flinkcdc.common.config.ErrorCodes;
import com.flinkcdc.common.config.MetricKeys;
import com.flinkcdc.common.dlq.DLQPublisher;
import com.flinkcdc.common.metric.Metrics;
import com.flinkcdc.common.model.CdcEnvelop;
import com.flinkcdc.common.model.DlqEvent;
import com.flinkcdc.common.pipeline.PipelineBuilder;
import com.flinkcdc.domain.sample1.Sample1Constants;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
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
                .map(new RichMapFunction<Document, CdcEnvelop>() {

                    private transient Metrics metrics;

                    @Override
                    public void open(Configuration parameters) {
                        metrics = new Metrics(getRuntimeContext(), Sample1Constants.JOB_NAME, Sample1Constants.PARSER_NAME);
                        DLQPublisher.getInstance().initMetrics(getRuntimeContext(), Sample1Constants.JOB_NAME);
                    }

                    @Override
                    public CdcEnvelop map(Document doc) {
                        try {
                            CdcEnvelop env = MAPPER.convertValue(doc, CdcEnvelop.class);
                            metrics.inc(MetricKeys.PARSER_SUCCESS_COUNT);
                            return env;
                        } catch (Exception e) {
                            metrics.inc(MetricKeys.PARSER_ERROR_COUNT);
                            log.warn("Failed to convert Mongo Document to CdcEnvelop: {}", doc, e);
                            DlqEvent dlqEvent = DlqEvent.of(
                                    ErrorCodes.PARSING_ERROR,
                                    e.getMessage(),
                                    Sample1Constants.PARSER_NAME,
                                    doc.toJson(),
                                    e
                            );
                            DLQPublisher.getInstance().publish(dlqEvent);
                            return null;
                        }
                    }
                })
                .filter(Objects::nonNull)
                .name(Sample1Constants.PARSER_NAME);
    }
}
