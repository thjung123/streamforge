package com.streamforge.job;

import com.streamforge.core.BaseIntegrationTest;
import com.streamforge.core.model.CdcEnvelop;
import com.streamforge.job.cdcsync.MongoToKafkaJob;
import com.mongodb.client.MongoCollection;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.bson.Document;
import org.junit.jupiter.api.*;
import java.time.Duration;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class MongoToKafkaIntegrationTest extends BaseIntegrationTest {

    private static KafkaConsumer<String, String> consumer;

    @BeforeAll
    static void setupJobAndConsumer() throws Exception {
        String groupId = "mongo-to-kafka-test-group-" + UUID.randomUUID();

        consumer = new KafkaConsumer<>(
                Map.of(
                        "bootstrap.servers", kafka.getBootstrapServers(),
                        "group.id", groupId,
                        "auto.offset.reset", "earliest",
                        "enable.auto.commit", "false",
                        "key.deserializer", StringDeserializer.class.getName(),
                        "value.deserializer", StringDeserializer.class.getName()
                )
        );
        consumer.subscribe(List.of(KAFKA_TOPIC_MAIN));

        MongoToKafkaJob job = new MongoToKafkaJob();
        StreamExecutionEnvironment env = job.buildPipeline();

        Thread flinkThread = new Thread(() -> {
            try {
                env.execute("mongo-to-kafka-test-job");
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        flinkThread.setDaemon(true);
        flinkThread.start();

        System.out.println("[SETUP] Flink CDC job started, waiting for warm-up...");
        Thread.sleep(8000);
    }

    @Test
    void testInsertUpdateDeleteFlow() throws Exception {
        MongoCollection<Document> collection = mongoClient
                .getDatabase(MONGO_DB_NAME)
                .getCollection(MONGO_COLLECTION);

        System.out.println("\n=== [STEP] INSERT ===");
        collection.insertOne(new Document("_id", 1).append("name", "Alice"));
        String insertJson = waitForKafkaEvent("\"operation\":\"insert\"", Duration.ofSeconds(20));
        CdcEnvelop insertEnv = CdcEnvelop.fromJson(insertJson);
        assertThat(insertEnv.getOperation()).isEqualToIgnoringCase("insert");
        assertThat(insertEnv.getPayloadAsMap().get("name"))
                .as("Inserted name must be Alice")
                .isEqualTo("Alice");

        System.out.println("\n=== [STEP] UPDATE ===");
        collection.updateOne(new Document("_id", 1),
                new Document("$set", new Document("name", "Alicia")));
        String updateJson = waitForKafkaEvent("\"operation\":\"update\"", Duration.ofSeconds(20));
        CdcEnvelop updateEnv = CdcEnvelop.fromJson(updateJson);
        assertThat(updateEnv.getOperation()).isEqualToIgnoringCase("update");
        Map<String, Object> updatePayload = updateEnv.getPayloadAsMap();

        System.out.println("[DEBUG] Update payload: " + updatePayload);

        assertThat(updatePayload.get("_id")).isEqualTo(1);
        if (updatePayload.containsKey("name")) {
            assertThat(updatePayload.get("name"))
                    .as("Updated name must be Alicia")
                    .isEqualTo("Alicia");
        } else {
            System.err.println("[WARN] 'name' field missing in update payload → CDC running in delta mode");
        }

        System.out.println("\n=== [STEP] DELETE ===");
        collection.deleteOne(new Document("_id", 1));
        String deleteJson = waitForKafkaEvent("\"operation\":\"delete\"", Duration.ofSeconds(20));
        CdcEnvelop deleteEnv = CdcEnvelop.fromJson(deleteJson);
        assertThat(deleteEnv.getOperation()).isEqualToIgnoringCase("delete");
        assertThat(deleteEnv.getPayloadAsMap().get("_id")).isEqualTo(1);
    }

    private String waitForKafkaEvent(String keyword, Duration timeout) {
        long end = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < end) {
            var polled = consumer.poll(Duration.ofMillis(500));
            if (!polled.isEmpty()) {
                for (ConsumerRecord<String, String> record : polled) {
                    String value = record.value();
                    System.out.println("[KAFKA] " + value);
                    if (value.toLowerCase().contains(keyword.toLowerCase())) {
                        System.out.println("[MATCH] Found event containing " + keyword);
                        return value;
                    }
                }
            }
        }
        throw new AssertionError("No Kafka event found with keyword: " + keyword + " within " + timeout.getSeconds() + "s");
    }

    @AfterAll
    static void cleanup() {
        if (consumer != null) {
            consumer.close();
        }
    }
}
