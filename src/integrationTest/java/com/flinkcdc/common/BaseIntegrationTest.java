package com.flinkcdc.common;

import com.mongodb.ReadConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static com.mongodb.client.model.Filters.eq;

@Testcontainers
public abstract class BaseIntegrationTest {

    @Container
    protected static final GenericContainer<?> zookeeper = new GenericContainer<>(
            DockerImageName.parse("confluentinc/cp-zookeeper:7.5.0")
    )
            .withEnv("ZOOKEEPER_CLIENT_PORT", "2181")
            .withEnv("ZOOKEEPER_TICK_TIME", "2000")
            .withExposedPorts(2181);

    @Container
    protected static final KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.5.0")
    )
            .dependsOn(zookeeper)
            .withEnv("KAFKA_ZOOKEEPER_CONNECT", "zookeeper:2181")
            .withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1");

    @Container
    protected static final MongoDBContainer mongo = new MongoDBContainer(
            DockerImageName.parse("mongo:6.0")
    )
            .withCommand("--replSet", "rs0", "--bind_ip_all")
            .withExposedPorts(27017);

    protected static MongoClient mongoClient;

    protected static final String KAFKA_TOPIC_MAIN = "cdc-topic";
    protected static final String KAFKA_TOPIC_DLQ = "cdc-dlq";
    protected static final String MONGO_DB_NAME = "cdc_test";
    protected static final String MONGO_COLLECTION = "events";

    @BeforeAll
    static void setUpEnvironment() throws Exception {
        kafka.start();
        mongo.start();

        String internalHost = mongo.execInContainer("hostname").getStdout().trim();
        int mappedPort = mongo.getMappedPort(27017);

        mongo.execInContainer(
                "mongosh", "--quiet", "--eval",
                String.format(
                        "rs.initiate({_id:'rs0', members:[{_id:0, host:'%s:27017'}]})",
                        internalHost
                )
        );

        waitUntilReplicaPrimary(Duration.ofSeconds(30));

        String mongoUri = String.format(
                "mongodb://localhost:%d/?replicaSet=rs0&directConnection=true",
                mappedPort
        );
        System.out.println("[INIT] Mongo URI: " + mongoUri);

        waitForMongoAvailable(mongoUri, Duration.ofSeconds(20));

        mongoClient = MongoClients.create(mongoUri);
        MongoDatabase db = mongoClient.getDatabase(MONGO_DB_NAME);

        if (!db.listCollectionNames().into(new ArrayList<>()).contains(MONGO_COLLECTION)) {
            db.createCollection(MONGO_COLLECTION);
        }

        createKafkaTopic(KAFKA_TOPIC_MAIN);
        createKafkaTopic(KAFKA_TOPIC_DLQ);

        System.setProperty("MONGO_URI", mongoUri);
        System.setProperty("MONGO_DB", MONGO_DB_NAME);
        System.setProperty("MONGO_COLLECTION", MONGO_COLLECTION);
        System.setProperty("KAFKA_BOOTSTRAP_SERVERS", kafka.getBootstrapServers());
        System.setProperty("CDC_TOPIC", KAFKA_TOPIC_MAIN);
        System.setProperty("CDC_DLQ_TOPIC", KAFKA_TOPIC_DLQ);

        System.out.println("[INIT] Test environment ready");
    }

    @AfterAll
    static void tearDownEnvironment() {
        try {
            Thread.sleep(1000);
        } catch (InterruptedException ignored) {}


        if (mongoClient != null) {
            System.out.println("[CLOSE] Closing Mongo client...");
            mongoClient.close();
        }
        kafka.stop();
        mongo.stop();
    }

    private static void waitForMongoAvailable(String uri, Duration timeout) throws InterruptedException {
        Instant start = Instant.now();
        while (Duration.between(start, Instant.now()).compareTo(timeout) < 0) {
            try (MongoClient client = MongoClients.create(uri)) {
                client.getDatabase("admin").runCommand(new Document("ping", 1));
                System.out.println("[CHECK] Mongo ready");
                return;
            } catch (Exception e) {
                Thread.sleep(1000);
            }
        }
        throw new IllegalStateException("Mongo ping timeout");
    }

    private static void waitUntilReplicaPrimary(Duration timeout) throws InterruptedException {
        Instant start = Instant.now();
        while (Duration.between(start, Instant.now()).compareTo(timeout) < 0) {
            try {
                var res = mongo.execInContainer(
                        "mongosh", "--quiet", "--eval", "rs.status().members[0].stateStr"
                );
                String state = res.getStdout().trim();
                if (state.contains("PRIMARY")) {
                    System.out.println("[CHECK] ReplicaSet PRIMARY");
                    return;
                }
            } catch (Exception ignored) {}
            Thread.sleep(1000);
        }
        throw new IllegalStateException("Replica set not PRIMARY");
    }

    private static void createKafkaTopic(String topicName) throws ExecutionException, InterruptedException {
        try (AdminClient admin = AdminClient.create(Map.of("bootstrap.servers", kafka.getBootstrapServers()))) {
            admin.createTopics(Collections.singletonList(new NewTopic(topicName, 1, (short) 1))).all().get();
            System.out.println("[INIT] Kafka topic: " + topicName);
        }
    }

    protected Document waitForMongoDocument(Object id, int timeoutSeconds, String expectedName) throws InterruptedException {
        MongoCollection<Document> collection = mongoClient
                .getDatabase(MONGO_DB_NAME)
                .getCollection(MONGO_COLLECTION)
                .withReadConcern(ReadConcern.MAJORITY);

        Instant start = Instant.now();
        while (Duration.between(start, Instant.now()).getSeconds() < timeoutSeconds) {
            Document doc = collection.find(eq("_id", id)).first();
            if (doc != null) {
                if (expectedName == null || expectedName.equals(doc.getString("name"))) {
                    return doc;
                }
            }
            Thread.sleep(1000);
        }
        return null;
    }

    protected Document waitUntilDeleted(Object id, int timeoutSeconds) throws InterruptedException {
        MongoCollection<Document> collection = mongoClient
                .getDatabase(MONGO_DB_NAME)
                .getCollection(MONGO_COLLECTION)
                .withReadConcern(ReadConcern.MAJORITY);

        Instant start = Instant.now();
        while (Duration.between(start, Instant.now()).getSeconds() < timeoutSeconds) {
            Document doc = collection.find(eq("_id", id)).first();
            if (doc == null) return null;
            Thread.sleep(1000);
        }
        return collection.find(eq("_id", id)).first();
    }


    protected void printAllDocs() {
        MongoCollection<Document> collection =
                mongoClient.getDatabase(MONGO_DB_NAME).getCollection(MONGO_COLLECTION);
        List<Document> docs = collection.find().into(new ArrayList<>());
        System.out.println("[DEBUG] Mongo docs: " + docs);
    }
}
