package com.flinkcdc.common.sink;

import com.flinkcdc.common.model.CdcEnvelop;
import com.flinkcdc.common.pipeline.PipelineBuilder;
import com.flinkcdc.common.utils.JsonUtils;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.bson.Document;

import static com.flinkcdc.common.config.ConfigKeys.*;
import static com.flinkcdc.common.config.ScopedConfig.*;

public class MongoSinkBuilder implements PipelineBuilder.SinkBuilder<CdcEnvelop> {

    @Override
    public DataStreamSink<CdcEnvelop> write(DataStream<CdcEnvelop> stream) {
        return stream.addSink(new MongoSinkFunction())
                .name("MongoSink");
    }

    static class MongoSinkFunction implements SinkFunction<CdcEnvelop> {

        private transient MongoClient client;
        private transient MongoCollection<Document> collection;

        @Override
        public void invoke(CdcEnvelop value, Context context) {
            if (client == null) {
                client = MongoClients.create(require(MONGO_URI));
                MongoDatabase db = client.getDatabase(require(MONGO_DB));
                collection = db.getCollection(require(MONGO_COLLECTION));
            }

            String json = JsonUtils.toJson(value);
            Document doc = Document.parse(json);
            collection.insertOne(doc);
        }
    }
}
