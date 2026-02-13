package com.streamforge.job;

import com.streamforge.core.BaseIntegrationTest;
import com.streamforge.core.model.CdcEnvelop;
import com.streamforge.job.cdcsync.KafkaToMongoJob;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.bson.Document;
import org.junit.jupiter.api.*;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class KafkaToMongoIntegrationTest extends BaseIntegrationTest {

    @BeforeAll
    static void startJob() throws Exception {
        KafkaToMongoJob job = new KafkaToMongoJob();
        StreamExecutionEnvironment env = job.buildPipeline();
        Thread flinkThread = new Thread(() -> {
            try {
                env.execute();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        flinkThread.setDaemon(true);
        flinkThread.start();

        Thread.sleep(3000);
    }
    @Test
    void testInsertUpdateDeleteFlow() throws Exception {
        sendCdcEvent("insert", Map.of("_id", 1, "name", "Charlie"));
        Document inserted = waitForMongoDocument(1, 10, "Charlie");
        printAllDocs();
        assertThat(inserted).isNotNull();
        assertThat(inserted.getString("name")).isEqualTo("Charlie");

        sendCdcEvent("update", Map.of("_id", 1, "name", "Charlotte"));
        Document updated = waitForMongoDocument(1, 10, "Charlotte");
        printAllDocs();
        assertThat(updated).isNotNull();
        assertThat(updated.getString("name")).isEqualTo("Charlotte");

        sendCdcEvent("delete", Map.of("_id", 1));
        Document deleted = waitUntilDeleted(1, 10);
        printAllDocs();
        assertThat(deleted).isNull();
    }



    private void sendCdcEvent(String op, Map<String, Object> payload) throws Exception {
        try (var producer = new KafkaProducer<String, String>(
                Map.of(
                        "bootstrap.servers", kafka.getBootstrapServers(),
                        "key.serializer", "org.apache.kafka.common.serialization.StringSerializer",
                        "value.serializer", "org.apache.kafka.common.serialization.StringSerializer"
                )
        )) {
            CdcEnvelop envelop = CdcEnvelop.of(op, "test-source", payload, "_id");
            envelop.setTraceId(UUID.randomUUID().toString());
            producer.send(new ProducerRecord<>(KAFKA_TOPIC_MAIN, envelop.toJson())).get();
        }
    }
}
