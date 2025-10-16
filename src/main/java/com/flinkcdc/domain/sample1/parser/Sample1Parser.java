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
import org.bson.BsonDocument;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class Sample1Parser implements PipelineBuilder.ParserFunction<Document, CdcEnvelop> {

    private static final Logger log = LoggerFactory.getLogger(Sample1Parser.class);
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

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
                            CdcEnvelop env = Sample1Parser.from(doc);
                            metrics.inc(MetricKeys.PARSER_SUCCESS_COUNT);
                            return env;
                        } catch (Exception e) {
                            metrics.inc(MetricKeys.PARSER_ERROR_COUNT);
                            log.warn("Failed to parse CDC Document to CdcEnvelop: {}", doc, e);

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

    public static CdcEnvelop from(Document doc) {
        try {
            String op = null;
            if (doc.containsKey("op")) {
                op = doc.getString("op");
            } else if (doc.containsKey("operationType")) {
                op = doc.getString("operationType");
            }

            assert op != null;
            String operation = switch (op) {
                case "c", "insert" -> "insert";
                case "u", "update", "replace" -> "update";
                case "d", "delete" -> "delete";
                default -> "unknown";
            };

            Map<String, Object> payload = null;
            if (doc.containsKey("after")) {
                payload = doc.get("after", Map.class);
            } else if (doc.containsKey("fullDocument")) {
                payload = doc.get("fullDocument", Map.class);
            }

            if ("update".equals(operation)) {
                if (payload == null && doc.containsKey("updateDescription")) {
                    Document updateDesc = doc.get("updateDescription", Document.class);
                    if (updateDesc != null && updateDesc.containsKey("updatedFields")) {
                        payload = updateDesc.get("updatedFields", Map.class);
                    }
                }

                if (doc.containsKey("documentKey")) {
                    Object documentKeyObj = doc.get("documentKey");
                    Document docKey = null;

                    if (documentKeyObj instanceof BsonDocument bsonKey) {
                        docKey = Document.parse(bsonKey.toJson());
                    } else if (documentKeyObj instanceof Document d) {
                        docKey = d;
                    }

                    if (docKey != null) {
                        if (payload == null) payload = new HashMap<>();
                        payload.putAll(docKey);
                    }
                }
            }

            if ("delete".equals(operation) && doc.containsKey("documentKey")) {
                Object documentKeyObj = doc.get("documentKey");
                Document docKey = null;

                if (documentKeyObj instanceof BsonDocument bsonKey) {
                    docKey = Document.parse(bsonKey.toJson());
                } else if (documentKeyObj instanceof Document d) {
                    docKey = d;
                }

                if (docKey != null) {
                    payload = new HashMap<>();
                    payload.putAll(docKey);
                }
            }

            String primaryKey = null;
            if (payload != null && payload.containsKey("_id")) {
                primaryKey = String.valueOf(payload.get("_id"));
            }

            return CdcEnvelop.of(
                    operation,
                    doc.getString("source") != null ? doc.getString("source") : "unknown",
                    payload,
                    primaryKey
            );

        } catch (Exception e) {
            log.warn("Failed to map Document to CdcEnvelop: {}", doc, e);
            throw e;
        }
    }
}
