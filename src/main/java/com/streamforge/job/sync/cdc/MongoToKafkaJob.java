package com.streamforge.job.sync.cdc;

import com.streamforge.connector.kafka.KafkaSinkBuilder;
import com.streamforge.connector.mongo.MongoChangeStreamSource;
import com.streamforge.core.config.ScopedConfig;
import com.streamforge.core.launcher.StreamJob;
import com.streamforge.core.model.StreamEnvelop;
import com.streamforge.core.pipeline.PipelineBuilder;
import com.streamforge.job.sync.cdc.parser.MongoToKafkaParser;
import com.streamforge.job.sync.cdc.processor.MongoToKafkaProcessor;
import com.streamforge.pattern.dedup.Deduplicator;
import com.streamforge.pattern.filter.FilterInterceptor;
import com.streamforge.pattern.merge.StatefulMerger;
import com.streamforge.pattern.observability.*;
import java.time.Duration;
import java.util.Set;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MongoToKafkaJob implements StreamJob {

  public static final String JOB_NAME = "MongoToKafka";
  public static final String PARSER_NAME = "MongoToKafkaParser";
  public static final String PROCESSOR_NAME = "MongoToKafkaProcessor";

  @Override
  public String name() {
    return JOB_NAME;
  }

  public StreamExecutionEnvironment buildPipeline() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    PipelineBuilder.from(new MongoChangeStreamSource().build(env, name()))
        .parse(new MongoToKafkaParser())
        .apply(new FlowDisruptionDetector<>(StreamEnvelop::getSource, Duration.ofMinutes(5)))
        .apply(new FilterInterceptor<>(e -> !"unknown".equals(e.getOperation())))
        .apply(
            new Deduplicator<>(
                e -> e.getPrimaryKey() + ":" + e.getEventTime(), Duration.ofMinutes(10)))
        .apply(
            new StatefulMerger<>(
                StreamEnvelop::getPrimaryKey,
                StreamEnvelop::getPayloadAsMap,
                Set.of("updatedAt", "modifiedAt")))
        .apply(new LatencyDetector<>(StreamEnvelop::getEventTime, Duration.ofSeconds(30)))
        .apply(
            new OnlineObserver<>(
                QualityCheck.of("null_payloads", e -> e.getPayloadJson() == null),
                QualityCheck.of("null_keys", e -> e.getPrimaryKey() == null)))
        .apply(new MetadataDecorator<>(StreamEnvelop::getMetadata, "pre-sink"))
        .process(new MongoToKafkaProcessor())
        .to(KafkaSinkBuilder.exactlyOnce("txn-" + name()), name());

    return env;
  }

  @Override
  public void run(String[] args) throws Exception {
    ScopedConfig.activateJob(name());
    try (StreamExecutionEnvironment env = buildPipeline()) {
      JobExecutionResult result = env.execute(name());
      System.out.println("Job duration: " + result.getNetRuntime());
    }
  }

  /** Direct Flink cluster submission: flink run -c ...MongoToKafkaJob streamforge.jar */
  public static void main(String[] args) throws Exception {
    new MongoToKafkaJob().run(args);
  }
}
