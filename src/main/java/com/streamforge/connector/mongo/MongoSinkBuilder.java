package com.streamforge.connector.mongo;

import static com.mongodb.client.model.Filters.eq;
import static com.streamforge.connector.mongo.MongoConfigKeys.*;
import static com.streamforge.core.config.ErrorCodes.SINK_ERROR;
import static com.streamforge.core.config.ScopedConfig.*;

import com.mongodb.MongoBulkWriteException;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.WriteModel;
import com.streamforge.core.config.MetricKeys;
import com.streamforge.core.dlq.DLQPublisher;
import com.streamforge.core.metric.Metrics;
import com.streamforge.core.model.DlqEvent;
import com.streamforge.core.model.StreamEnvelop;
import com.streamforge.core.pipeline.PipelineBuilder;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongoSinkBuilder implements PipelineBuilder.SinkBuilder<StreamEnvelop> {

  public static final String OPERATOR_NAME = "MongoSink";

  @Override
  public DataStreamSink<StreamEnvelop> write(DataStream<StreamEnvelop> stream, String jobName) {
    String uri = require(MONGO_URI);
    String db = require(MONGO_DB);
    String collection = require(MONGO_COLLECTION);
    return stream.sinkTo(new MongoSink(jobName, uri, db, collection)).name(OPERATOR_NAME);
  }

  static class MongoSink implements Sink<StreamEnvelop> {
    private final String jobName;
    private final String uri;
    private final String db;
    private final String collection;

    public MongoSink(String jobName, String uri, String db, String collection) {
      this.jobName = jobName;
      this.uri = uri;
      this.db = db;
      this.collection = collection;
    }

    @SuppressWarnings("deprecation")
    @Override
    public SinkWriter<StreamEnvelop> createWriter(InitContext context) {
      return new MongoSinkWriter(jobName, context, uri, db, collection);
    }
  }

  static class MongoSinkWriter implements SinkWriter<StreamEnvelop> {

    private static final Logger log = LoggerFactory.getLogger(MongoSinkWriter.class);
    private static final int BATCH_SIZE = 500;
    private static final long FLUSH_INTERVAL_MS = 1000;

    private final MongoClient client;
    private final MongoCollection<Document> collection;
    private final Metrics metrics;
    private final String jobName;
    private final ProcessingTimeService timeService;
    private final List<WriteModel<Document>> batch = new ArrayList<>();
    private final List<StreamEnvelop> pending = new ArrayList<>();

    @SuppressWarnings("deprecation")
    MongoSinkWriter(
        String jobName, Sink.InitContext context, String uri, String db, String collection) {
      this.jobName = jobName;
      this.metrics = new Metrics(context.metricGroup(), jobName, MetricKeys.MONGO);
      this.client = MongoClients.create(uri);
      this.collection = client.getDatabase(db).getCollection(collection);
      this.timeService = context.getProcessingTimeService();
      scheduleFlush();
      log.info("[MongoSink] Initialized for job={} collection={}", jobName, collection);
    }

    // for unit tests
    MongoSinkWriter(String jobName, MongoCollection<Document> collection) {
      this.jobName = jobName;
      this.client = null;
      this.collection = collection;
      this.metrics = null;
      this.timeService = null;
    }

    @Override
    public void write(StreamEnvelop value, Context context) {
      WriteModel<Document> model;
      try {
        String op = value.getOperation();
        Object pkValue =
            value.getPayloadAsMap() != null && value.getPrimaryKey() != null
                ? value.getPayloadAsMap().get(value.getPrimaryKey())
                : null;

        if (pkValue == null) {
          log.warn("[MongoSink] Missing PK value: {}", value);
          if (metrics != null) metrics.inc(MetricKeys.SINK_ERROR_COUNT);
          return;
        }

        if ("DELETE".equalsIgnoreCase(op)) {
          model = new DeleteOneModel<>(eq("_id", pkValue));
        } else {
          Document doc = Document.parse(value.getPayloadJson());
          doc.put("_id", pkValue);
          model = new ReplaceOneModel<>(eq("_id", pkValue), doc, new ReplaceOptions().upsert(true));
        }
      } catch (Exception e) {
        if (metrics != null) metrics.inc(MetricKeys.SINK_ERROR_COUNT);
        log.error("[MongoSink] Failed to build write model, routing to DLQ", e);
        publishToDlq(value, e);
        return;
      }

      batch.add(model);
      pending.add(value);
      if (batch.size() >= BATCH_SIZE) {
        flushBatch();
      }
    }

    @Override
    public void flush(boolean endOfInput) {
      flushBatch();
    }

    private void scheduleFlush() {
      if (timeService == null) {
        return;
      }
      timeService.registerTimer(
          timeService.getCurrentProcessingTime() + FLUSH_INTERVAL_MS,
          ts -> {
            flushBatch();
            scheduleFlush();
          });
    }

    private void flushBatch() {
      if (batch.isEmpty()) {
        return;
      }
      List<WriteModel<Document>> models = new ArrayList<>(batch);
      List<StreamEnvelop> values = new ArrayList<>(pending);
      batch.clear();
      pending.clear();

      try {
        collection.bulkWrite(models, new BulkWriteOptions().ordered(false));
        incSuccess(models.size());
      } catch (MongoBulkWriteException e) {
        Set<Integer> failed = new HashSet<>();
        for (BulkWriteError err : e.getWriteErrors()) {
          failed.add(err.getIndex());
        }
        for (int i = 0; i < values.size(); i++) {
          if (failed.contains(i)) {
            if (metrics != null) metrics.inc(MetricKeys.SINK_ERROR_COUNT);
            publishToDlq(values.get(i), e);
          } else {
            incSuccess(1);
          }
        }
        log.error("[MongoSink] {} of {} writes failed in bulk", failed.size(), values.size(), e);
      } catch (Exception e) {
        log.error("[MongoSink] Bulk write failed", e);
        for (StreamEnvelop value : values) {
          if (metrics != null) metrics.inc(MetricKeys.SINK_ERROR_COUNT);
          publishToDlq(value, e);
        }
      }
    }

    private void incSuccess(int count) {
      if (metrics == null) {
        return;
      }
      for (int i = 0; i < count; i++) {
        metrics.inc(MetricKeys.SINK_SUCCESS_COUNT);
      }
    }

    private void publishToDlq(StreamEnvelop value, Exception e) {
      DLQPublisher.getInstance()
          .publish(
              DlqEvent.of(
                  SINK_ERROR,
                  e.getMessage(),
                  OPERATOR_NAME,
                  value != null ? value.toJson() : null,
                  e));
    }

    @Override
    public void close() {
      try {
        flushBatch();
      } catch (Exception e) {
        log.warn("[MongoSink] Failed to flush on close for job={}", jobName, e);
      }
      if (client != null) {
        try {
          client.close();
          log.info("[MongoSink] MongoClient closed for job={}", jobName);
        } catch (Exception e) {
          log.warn("[MongoSink] Failed to close MongoClient for job={}", jobName, e);
        }
      }
    }
  }
}
