package com.streamforge.connector.mongo;

import com.streamforge.core.dlq.DLQPublisher;
import com.streamforge.core.model.DlqEvent;
import com.streamforge.core.pipeline.PipelineBuilder;
import com.streamforge.connector.mongo.util.NoSplit;
import com.streamforge.connector.mongo.util.NoSplitEnumerator;
import com.streamforge.connector.mongo.util.NoSplitSerializer;
import com.mongodb.MongoException;
import com.mongodb.client.*;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.bson.BsonDocument;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.streamforge.connector.mongo.MongoConfigKeys.*;
import static com.streamforge.core.config.ScopedConfig.getOrDefault;
import static com.streamforge.core.config.ScopedConfig.require;

public class CustomMongoCdcSource implements PipelineBuilder.SourceBuilder<Document> {

    @Override
    public DataStream<Document> build(StreamExecutionEnvironment env, String jobName) {
        return env.fromSource(
                new MongoChangeStreamSource(),
                WatermarkStrategy.noWatermarks(),
                jobName + "-MongoCDC"
        );
    }

    public static class MongoChangeStreamSource implements Source<Document, NoSplit, Void> {

        @Override
        public Boundedness getBoundedness() {
            return Boundedness.CONTINUOUS_UNBOUNDED;
        }

        @Override
        public SourceReader<Document, NoSplit> createReader(SourceReaderContext ctx) throws Exception {
            return new MongoChangeStreamReader();
        }

        @Override
        public SplitEnumerator<NoSplit, Void> createEnumerator(SplitEnumeratorContext<NoSplit> ctx) {
            return new NoSplitEnumerator<>(ctx);
        }

        @Override
        public SplitEnumerator<NoSplit, Void> restoreEnumerator(SplitEnumeratorContext<NoSplit> ctx, Void checkpoint) {
            return new NoSplitEnumerator<>(ctx);
        }

        @Override
        public SimpleVersionedSerializer<NoSplit> getSplitSerializer() {
            return NoSplitSerializer.INSTANCE;
        }

        @Override
        public SimpleVersionedSerializer<Void> getEnumeratorCheckpointSerializer() {
            return new SimpleVersionedSerializer<Void>() {
                @Override
                public int getVersion() {
                    return 1;
                }

                @Override
                public byte[] serialize(Void state) {
                    return new byte[0];
                }

                @Override
                public Void deserialize(int version, byte[] serialized) {
                    return null;
                }
            };
        }
    }

    public static class MongoChangeStreamReader implements SourceReader<Document, NoSplit> {
        private static final Logger log = LoggerFactory.getLogger(MongoChangeStreamReader.class);

        private MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor;
        private volatile boolean running = true;
        private MongoClient client;

        @Override
        public void start() {
            try {
                String uri = require(MONGO_URI);
                String dbName = getOrDefault(MONGO_DB, MONGO_DB);
                String collName = getOrDefault(MONGO_COLLECTION, MONGO_COLLECTION);

                client = MongoClients.create(uri);
                MongoDatabase db = client.getDatabase(dbName);
                MongoCollection<Document> coll = db.getCollection(collName);

                for (Document doc : coll.find().limit(3)) {
                    System.out.println("[INIT] Sample doc: " + doc.toJson());
                }

                MongoCollection<Document> collection = client.getDatabase(dbName).getCollection(collName);
                cursor = collection.watch().cursor();
                log.info("[MongoCDC] Connected to {}.{} via {}", db, coll, uri);
            } catch (Exception e) {
                log.error("[MongoCDC] Failed to start ChangeStream reader", e);
                throw new RuntimeException("Failed to start Mongo CDC source", e);
            }
        }

        @Override
        public InputStatus pollNext(ReaderOutput<Document> output) throws Exception {
            if (!running || cursor == null) {
                return InputStatus.END_OF_INPUT;
            }

            try {
                if (!cursor.hasNext()) {
                    Thread.sleep(100);
                    return InputStatus.NOTHING_AVAILABLE;
                }

                ChangeStreamDocument<Document> change = cursor.next();
                String dbName = change.getNamespace() != null ? change.getNamespace().getDatabaseName() : null;
                String collName = change.getNamespace() != null ? change.getNamespace().getCollectionName() : null;

                Document fullDoc = change.getFullDocument();
                Document fullDocument = null;
                if (fullDoc != null) {
                    fullDocument = Document.parse(fullDoc.toJson());
                }

                Document event = new Document()
                        .append("op", change.getOperationType() != null ? change.getOperationType().getValue() : "unknown")
                        .append("db", dbName)
                        .append("collection", collName)
                        .append("documentKey", change.getDocumentKey())
                        .append("fullDocument", fullDocument)
                        .append("eventTime", change.getClusterTime());

                output.collect(event);
                return InputStatus.MORE_AVAILABLE;
            } catch (MongoException ex) {
                log.warn("[MongoCDC] Lost connection to MongoDB, retrying in 3s...", ex);
                safeCloseCursor();
                Thread.sleep(3000);
                reconnect();
                return InputStatus.NOTHING_AVAILABLE;
            } catch (Exception e) {
                log.error("[MongoCDC] Error reading change stream event", e);
                DlqEvent dlqEvent = DlqEvent.of(
                        "SOURCE_PARSING_ERROR",
                        e.getMessage(),
                        "MongoChangeStreamSource",
                        null,
                        e
                );
                DLQPublisher.getInstance().publish(dlqEvent);
                throw e;
            }
        }

        @Override
        public List<NoSplit> snapshotState(long checkpointId) {
            return Collections.emptyList();
        }

        @Override
        public void addSplits(List<NoSplit> splits) {
        }

        @Override
        public void notifyNoMoreSplits() {
        }

        @Override
        public CompletableFuture<Void> isAvailable() {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void close() {
            running = false;
            try {
                if (cursor != null) cursor.close();
                if (client != null) client.close();
                log.info("[MongoCDC] ChangeStream closed gracefully");
            } catch (Exception e) {
                log.warn("[MongoCDC] Error while closing resources", e);
            }
        }


        private void reconnect() {
            String uri = require(MONGO_URI);
            String dbName = getOrDefault(MONGO_DB, MONGO_DB);
            String collName = getOrDefault(MONGO_COLLECTION, MONGO_COLLECTION);
            client = MongoClients.create(uri);
            cursor = client.getDatabase(dbName).getCollection(collName).watch().cursor();
            log.info("[MongoCDC] Reconnected to MongoDB: {}.{}", dbName, collName);
        }

        private void safeCloseCursor() {
            try { if (cursor != null) cursor.close(); } catch (Exception ignored) {}
            try { if (client != null) client.close(); } catch (Exception ignored) {}
        }
    }

}
