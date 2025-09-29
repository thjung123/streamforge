package com.flinkcdc.common.source;

import com.flinkcdc.common.pipeline.PipelineBuilder;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.mongodb.source.MongoSource;
import org.apache.flink.connector.mongodb.source.reader.deserializer.MongoDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.bson.BsonDocument;
import org.bson.Document;

import java.io.Serial;
import java.io.Serializable;

import static com.flinkcdc.common.config.ConfigKeys.*;
import static com.flinkcdc.common.config.ScopedConfig.*;

public class MongoSourceBuilder implements PipelineBuilder.SourceBuilder<Document>, Serializable {
    @Serial
    private static final long serialVersionUID = 1L;

    @Override
    public DataStream<Document> build(StreamExecutionEnvironment env) {
        MongoSource<Document> source = createSource();
        return env.fromSource(source, WatermarkStrategy.noWatermarks(), "MongoDBSource");
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
