package com.flinkcdc.common.sink;

import com.flinkcdc.common.model.CdcEnvelop;
import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.LinkedHashMap;
import java.util.Map;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class MongoSinkBuilderTest {

    @Test
    void testInvokeCallsInsertOne() throws Exception {
        // given
        @SuppressWarnings("unchecked")
        MongoCollection<Document> mockCollection = Mockito.mock(MongoCollection.class);
        MongoSinkBuilder.MongoSinkFunction sink = new MongoSinkBuilder.MongoSinkFunction(mockCollection);

        sink.collection = mockCollection;

        CdcEnvelop envelop = CdcEnvelop.builder()
                .operation("INSERT")
                .payload(new LinkedHashMap<>(Map.of("id", 1, "name", "Charlie")))
                .build();

        // when
        sink.invoke(envelop, null);

        // then
        verify(mockCollection, times(1)).insertOne(any(Document.class));
    }
}
