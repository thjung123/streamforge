package com.streamforge.job.cdcsync.parser;

import com.streamforge.core.config.ErrorCodes;
import com.streamforge.core.config.MetricKeys;
import com.streamforge.core.dlq.DLQPublisher;
import com.streamforge.core.metric.Metrics;
import com.streamforge.core.model.CdcEnvelop;
import com.streamforge.core.model.DlqEvent;
import com.streamforge.core.pipeline.PipelineBuilder;
import com.streamforge.job.cdcsync.MongoToKafkaJob;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.bson.BsonDocument;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongoToKafkaParser implements PipelineBuilder.ParserFunction<Document, CdcEnvelop> {

  private static final Logger log = LoggerFactory.getLogger(MongoToKafkaParser.class);

  @Override
  public DataStream<CdcEnvelop> parse(DataStream<Document> input) {
    return input
        .map(
            new RichMapFunction<Document, CdcEnvelop>() {

              private transient Metrics metrics;

              @Override
              public void open(Configuration parameters) {
                metrics =
                    new Metrics(
                        getRuntimeContext(), MongoToKafkaJob.JOB_NAME, MongoToKafkaJob.PARSER_NAME);
                DLQPublisher.getInstance()
                    .initMetrics(getRuntimeContext(), MongoToKafkaJob.JOB_NAME);
              }

              @Override
              public CdcEnvelop map(Document doc) {
                try {
                  CdcEnvelop env = MongoToKafkaParser.from(doc);
                  metrics.inc(MetricKeys.PARSER_SUCCESS_COUNT);
                  return env;
                } catch (Exception e) {
                  metrics.inc(MetricKeys.PARSER_ERROR_COUNT);
                  log.warn("Failed to parse CDC Document to CdcEnvelop: {}", doc, e);

                  DlqEvent dlqEvent =
                      DlqEvent.of(
                          ErrorCodes.PARSING_ERROR,
                          e.getMessage(),
                          MongoToKafkaJob.PARSER_NAME,
                          doc.toJson(),
                          e);
                  DLQPublisher.getInstance().publish(dlqEvent);
                  return null;
                }
              }
            })
        .filter(Objects::nonNull)
        .name(MongoToKafkaJob.PARSER_NAME);
  }

  @SuppressWarnings("unchecked")
  public static CdcEnvelop from(Document doc) {
    try {
      String op = null;
      if (doc.containsKey("op")) {
        op = doc.getString("op");
      } else if (doc.containsKey("operationType")) {
        op = doc.getString("operationType");
      }

      assert op != null;
      String operation =
          switch (op) {
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
          payload = new HashMap<>(docKey);
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
          primaryKey);

    } catch (Exception e) {
      log.warn("Failed to map Document to CdcEnvelop: {}", doc, e);
      throw e;
    }
  }
}
