package com.flinkcdc.common.sink;

import com.flinkcdc.common.model.CdcEnvelop;
import com.flinkcdc.common.utils.JsonUtils;
import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class MongoSinkBuilderTest {

    @Test
    void testInvokeCallsInsertOne() throws Exception {
        MongoCollection<Document> mockCollection = Mockito.mock(MongoCollection.class);
        MongoSinkBuilder.MongoSinkFunction sink = new MongoSinkBuilder.MongoSinkFunction(mockCollection);
        sink.collection = mockCollection;

        String payloadJson = JsonUtils.toJson(Map.of("id", 1, "name", "Charlie"));

        CdcEnvelop envelop = CdcEnvelop.builder()
                .operation("INSERT")
                .payloadJson(payloadJson)
                .build();

        sink.invoke(envelop, null);

        verify(mockCollection, times(1)).insertOne(any(Document.class));
    }
}
