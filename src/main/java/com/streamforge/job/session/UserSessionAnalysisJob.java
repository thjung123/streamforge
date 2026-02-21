package com.streamforge.job.session;

import static com.streamforge.connector.kafka.KafkaConfigKeys.*;

import com.streamforge.connector.kafka.KafkaSourceBuilder;
import com.streamforge.connector.mongo.MongoSinkBuilder;
import com.streamforge.core.config.ScopedConfig;
import com.streamforge.core.launcher.StreamJob;
import com.streamforge.core.model.StreamEnvelop;
import com.streamforge.core.pipeline.PipelineBuilder;
import com.streamforge.core.util.JsonUtils;
import com.streamforge.job.cdc.parser.KafkaToMongoParser;
import com.streamforge.pattern.session.SessionAnalyzer;
import com.streamforge.pattern.session.SessionResult;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class UserSessionAnalysisJob implements StreamJob {

  public static final String JOB_NAME = "UserSessionAnalysis";
  public static final String SESSION_GAP_SECONDS = "SESSION_GAP_SECONDS";

  @Override
  public String name() {
    return JOB_NAME;
  }

  public StreamExecutionEnvironment buildPipeline() {
    return buildPipeline(new MongoSinkBuilder());
  }

  public StreamExecutionEnvironment buildPipeline(
      PipelineBuilder.SinkBuilder<StreamEnvelop> sinkBuilder) {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    ScopedConfig.require(STREAM_TOPIC);
    DataStream<String> raw = new KafkaSourceBuilder().build(env, name());
    DataStream<StreamEnvelop> events = new KafkaToMongoParser().parse(raw);

    Duration gap =
        Duration.ofSeconds(Long.parseLong(ScopedConfig.getOrDefault(SESSION_GAP_SECONDS, "1800")));

    DataStream<SessionResult<String>> sessions = buildAnalyzer(gap).apply(events);
    DataStream<StreamEnvelop> output = sessions.map(UserSessionAnalysisJob::toEnvelop);

    sinkBuilder.write(output, name());

    return env;
  }

  static SessionAnalyzer<StreamEnvelop, String> buildAnalyzer(Duration gap) {
    return SessionAnalyzer.<StreamEnvelop, String>builder()
        .keyExtractor(StreamEnvelop::getPrimaryKey)
        .timestampExtractor(e -> e.getEventTime().toEpochMilli())
        .aggregator(
            events -> {
              String actions =
                  events.stream().map(StreamEnvelop::getOperation).collect(Collectors.joining(","));
              return JsonUtils.toJson(Map.of("actions", actions, "count", events.size()));
            })
        .gap(gap)
        .build();
  }

  @SuppressWarnings("unchecked")
  static StreamEnvelop toEnvelop(SessionResult<String> result) {
    Map<String, Object> payload = JsonUtils.fromJson(result.result(), Map.class);
    payload.put("_id", result.key());

    Map<String, String> metadata = new HashMap<>();
    metadata.put("sessionStart", result.sessionStart().toString());
    metadata.put("sessionEnd", result.sessionEnd().toString());
    metadata.put("eventCount", String.valueOf(result.eventCount()));
    metadata.put("duration", result.duration().toString());

    return StreamEnvelop.builder()
        .operation("SESSION_CLOSED")
        .source("session-analyzer")
        .payloadJson(JsonUtils.toJson(payload))
        .primaryKey("_id")
        .eventTime(result.sessionStart())
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
    new UserSessionAnalysisJob().run(args);
  }
}
