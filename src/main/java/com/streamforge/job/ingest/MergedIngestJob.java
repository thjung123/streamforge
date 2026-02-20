package com.streamforge.job.ingest;

import static com.streamforge.connector.kafka.KafkaConfigKeys.*;
import static com.streamforge.core.config.ScopedConfig.*;

import com.streamforge.connector.kafka.KafkaSourceBuilder;
import com.streamforge.connector.mongo.MongoSinkBuilder;
import com.streamforge.core.config.ScopedConfig;
import com.streamforge.core.launcher.StreamJob;
import com.streamforge.core.model.StreamEnvelop;
import com.streamforge.core.pipeline.PipelineBuilder;
import com.streamforge.job.cdc.parser.KafkaToMongoParser;
import com.streamforge.pattern.split.OrderedFanIn;
import java.time.Duration;
import java.util.HashMap;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MergedIngestJob implements StreamJob {

  public static final String JOB_NAME = "MergedIngest";
  public static final String STREAM_TOPIC_SECONDARY = "STREAM_TOPIC_SECONDARY";

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

    require(STREAM_TOPIC);
    DataStream<String> ordersRaw = new KafkaSourceBuilder().build(env, name());
    DataStream<StreamEnvelop> orders = new KafkaToMongoParser().parse(ordersRaw);

    String secondaryTopic = require(STREAM_TOPIC_SECONDARY);
    DataStream<String> paymentsRaw =
        new KafkaSourceBuilder().build(env, name() + "-secondary", secondaryTopic);
    DataStream<StreamEnvelop> payments = new KafkaToMongoParser().parse(paymentsRaw);

    DataStream<StreamEnvelop> merged =
        OrderedFanIn.builder(StreamEnvelop::getEventTime)
            .source("orders", orders)
            .source("payments", payments)
            .maxDrift(Duration.ofSeconds(5))
            .sourceTag(
                (sourceName, element) -> {
                  if (element.getMetadata() == null) {
                    element.setMetadata(new HashMap<>());
                  }
                  element.getMetadata().put("ingestSource", sourceName);
                  return element;
                })
            .build()
            .merge();

    sinkBuilder.write(merged, name());

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

  public static void main(String[] args) throws Exception {
    new MergedIngestJob().run(args);
  }
}
