package com.streamforge.connector.mongo;

import com.streamforge.core.config.MetricKeys;
import com.streamforge.core.metric.Metrics;
import com.streamforge.core.pipeline.PipelineBuilder;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.mongodb.source.MongoSource;
import org.apache.flink.connector.mongodb.source.reader.deserializer.MongoDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.bson.BsonDocument;
import org.bson.Document;

import java.io.Serial;
import java.io.Serializable;

import static com.streamforge.connector.mongo.MongoConfigKeys.*;
import static com.streamforge.core.config.ScopedConfig.*;

public class MongoSourceBuilder implements PipelineBuilder.SourceBuilder<Document>, Serializable {
    @Serial
    private static final long serialVersionUID = 1L;
    public static final String OPERATOR_NAME = "MongoSource";

    @Override
    public DataStream<Document> build(StreamExecutionEnvironment env, String jobName) {
        MongoSource<Document> source = createSource();

        return env.fromSource(source, WatermarkStrategy.noWatermarks(), jobName + "-" + OPERATOR_NAME)
                .map(new MetricCountingMap(jobName))
                .name(OPERATOR_NAME);
    }

    static class MetricCountingMap extends RichMapFunction<Document, Document> {
        private final String jobName;
        private transient Metrics metrics;

        public MetricCountingMap(String jobName) {
            this.jobName = jobName;
        }

        @Override
        public void open(Configuration parameters) {
            metrics = new Metrics(getRuntimeContext(), jobName, MetricKeys.MONGO);
        }

        @Override
        public Document map(Document doc) {
            try {
                metrics.inc(MetricKeys.SOURCE_READ_COUNT);
                return doc;
            } catch (Exception e) {
                metrics.inc(MetricKeys.SOURCE_ERROR_COUNT);
                throw e;
            }
        }
    }

    protected MongoSource<Document> createSource() {
        String uri = require(MONGO_URI);
        String dbName = require(MONGO_DB);
        String collection = require(MONGO_COLLECTION);

        return MongoSource.<Document>builder()
                .setUri(uri)
                .setDatabase(dbName)
                .setCollection(collection)
                .setDeserializationSchema(new MongoDeserializationSchema<Document>() {
                    @Override
                    public Document deserialize(BsonDocument bson) {
                        return Document.parse(bson.toJson());
                    }

                    @Override
                    public TypeInformation<Document> getProducedType() {
                        return TypeInformation.of(Document.class);
                    }
                })
                .build();
    }
}
