package com.flinkcdc;

import com.flinkcdc.common.model.CdcEnvelop;
import com.flinkcdc.domain.sample2.job.Sample2Job;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.bson.Document;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class KafkaToMongoIntegrationTest {

    @Container
    static final KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.5.0")
    );

    @Container
    static final MongoDBContainer mongo = new MongoDBContainer(
            DockerImageName.parse("mongo:6.0")
    );

    private static MongoClient mongoClient;

    @BeforeAll
    static void setup() {
        mongo.start();
        kafka.start();

        mongoClient = MongoClients.create(mongo.getConnectionString());
        System.setProperty("KAFKA_BOOTSTRAP_SERVERS", kafka.getBootstrapServers());
        System.setProperty("KAFKA_SOURCE_TOPIC", "cdc-topic");
        System.setProperty("MONGO_URI", mongo.getConnectionString());
        System.setProperty("MONGO_DB", "cdc_test");
        System.setProperty("MONGO_COLLECTION", "events");
    }

    @Test
    @Order(1)
    @DisplayName("Kafka to Mongo end-to-end flow")
    void testKafkaToMongoEndToEnd() throws Exception {
        sendTestEventToKafka();

        Sample2Job job = new Sample2Job();
        StreamExecutionEnvironment env = job.buildPipeline();
        env.executeAsync();

        Thread.sleep(Duration.ofSeconds(10).toMillis());

        MongoCollection<Document> collection = mongoClient
                .getDatabase("cdc_test")
                .getCollection("events");

        List<Document> results = collection.find().into(new ArrayList<>());
        assertThat(results).isNotEmpty();
        assertThat(results.get(0).getString("operation")).isEqualTo("INSERT");
    }

    private void sendTestEventToKafka() throws Exception {
        try (var producer = new org.apache.kafka.clients.producer.KafkaProducer<String, String>(
                Map.of(
                        "bootstrap.servers", kafka.getBootstrapServers(),
                        "key.serializer", "org.apache.kafka.common.serialization.StringSerializer",
                        "value.serializer", "org.apache.kafka.common.serialization.StringSerializer"
                )
        )) {
            Map<String, Object> payloadMap = new HashMap<>();
            payloadMap.put("id", 1);
            payloadMap.put("name", "Charlie");



            CdcEnvelop envelop = CdcEnvelop.of("INSERT", "test-source", payloadMap, "id");
            envelop.setTraceId("test-trace");

            String json = envelop.toJson();
            producer.send(new ProducerRecord<>("cdc-topic", json)).get();
        }
    }

    @AfterAll
    static void teardown() {
        if (mongoClient != null) {
            mongoClient.close();
        }
        kafka.stop();
        mongo.stop();
    }
}
