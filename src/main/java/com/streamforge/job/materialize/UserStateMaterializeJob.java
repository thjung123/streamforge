package com.streamforge.job.materialize;

import static com.streamforge.connector.kafka.KafkaConfigKeys.*;

import com.streamforge.connector.kafka.KafkaSourceBuilder;
import com.streamforge.core.config.ScopedConfig;
import com.streamforge.core.launcher.StreamJob;
import com.streamforge.core.model.StreamEnvelop;
import com.streamforge.core.parser.StreamEnvelopParser;
import com.streamforge.core.util.JsonUtils;
import com.streamforge.pattern.materialization.ChangelogEvent;
import com.streamforge.pattern.materialization.Materializer;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class UserStateMaterializeJob implements StreamJob {

  public static final String JOB_NAME = "UserStateMaterialize";
  public static final String STATE_TTL_HOURS = "STATE_TTL_HOURS";
  public static final String CHANGELOG_TOPIC = "CHANGELOG_TOPIC";

  @Override
  public String name() {
    return JOB_NAME;
  }

  public StreamExecutionEnvironment buildPipeline() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    ScopedConfig.require(STREAM_TOPIC);
    String changelogTopic = ScopedConfig.require(CHANGELOG_TOPIC);

    DataStream<String> raw = new KafkaSourceBuilder().build(env, name());
    DataStream<StreamEnvelop> events = new StreamEnvelopParser(JOB_NAME).parse(raw);

    Duration ttl =
        Duration.ofHours(Long.parseLong(ScopedConfig.getOrDefault(STATE_TTL_HOURS, "24")));

    DataStream<ChangelogEvent<StreamEnvelop>> changelog = buildMaterializer(ttl).apply(events);
    DataStream<StreamEnvelop> output = changelog.map(UserStateMaterializeJob::toEnvelop);

    output.sinkTo(buildChangelogSink(changelogTopic)).name("ChangelogKafkaSink");

    return env;
  }

  static Materializer<StreamEnvelop> buildMaterializer(Duration ttl) {
    return Materializer.<StreamEnvelop>builder()
        .keyExtractor(StreamEnvelop::getPrimaryKey)
        .ttl(ttl)
        .deletePredicate(e -> "DELETE".equalsIgnoreCase(e.getOperation()))
        .build();
  }

  private KafkaSink<StreamEnvelop> buildChangelogSink(String topic) {
    return KafkaSink.<StreamEnvelop>builder()
        .setBootstrapServers(ScopedConfig.require(KAFKA_BOOTSTRAP_SERVERS))
        .setRecordSerializer(
            KafkaRecordSerializationSchema.<StreamEnvelop>builder()
                .setTopic(topic)
                .setValueSerializationSchema(envelop -> JsonUtils.toJson(envelop).getBytes())
                .build())
        .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build();
  }

  static StreamEnvelop toEnvelop(ChangelogEvent<StreamEnvelop> event) {
    Map<String, Object> payload = new HashMap<>();
    payload.put("_id", event.key());
    payload.put("changeType", event.type().name());

    if (event.after() != null) {
      payload.put("after", event.after().getPayloadAsMap());
    }
    if (event.before() != null) {
      payload.put("before", event.before().getPayloadAsMap());
    }

    Map<String, String> metadata = new HashMap<>();
    metadata.put("changeType", event.type().name());
    metadata.put("materializedKey", event.key());

    return StreamEnvelop.builder()
        .operation("CHANGELOG_" + event.type().name())
        .source("materializer")
        .payloadJson(JsonUtils.toJson(payload))
        .primaryKey("_id")
        .eventTime(event.timestamp())
        .processedTime(Instant.now())
        .metadata(metadata)
        .build();
  }

  @Override
  public void run(String[] args) throws Exception {
    ScopedConfig.activateJob(name());
    try (StreamExecutionEnvironment env = buildPipeline()) {
      JobExecutionResult result = env.execute(name());
      System.out.println("Job duration: " + result.getNetRuntime());
    }
  }

  public static void main(String[] args) throws Exception {
    new UserStateMaterializeJob().run(args);
  }
}
