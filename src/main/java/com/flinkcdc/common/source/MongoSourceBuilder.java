package com.flinkcdc.common.source;

import com.flinkcdc.common.pipeline.PipelineBuilder;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

import static com.flinkcdc.common.config.ConfigKeys.*;
import static com.flinkcdc.common.config.ScopedConfig.*;

public class MongoSourceBuilder implements PipelineBuilder.SourceBuilder<Document> {

    @Override
    public DataStream<Document> build(StreamExecutionEnvironment env) {
        String uri = require(MONGO_URI);
        String dbName = require(MONGO_DB);
        String collectionName = require(MONGO_COLLECTION);

        List<Document> docs = readCollection(uri, dbName, collectionName);
        return env.fromCollection(docs, TypeInformation.of(Document.class))
                .name("MongoSource")
                .assignTimestampsAndWatermarks(WatermarkStrategy.noWatermarks());
    }

    private List<Document> readCollection(String uri, String dbName, String collectionName) {
        List<Document> docs = new ArrayList<>();

        try (MongoClient client = MongoClients.create(uri)) {
            MongoCollection<Document> collection = client.getDatabase(dbName).getCollection(collectionName);

            try (MongoCursor<Document> cursor = collection.find().iterator()) {
                while (cursor.hasNext()) {
                    docs.add(cursor.next());
                }
            }
        }

        return docs;
    }
}
