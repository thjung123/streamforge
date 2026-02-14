package com.streamforge.job.sync.cdc;

import com.streamforge.connector.kafka.KafkaSinkBuilder;
import com.streamforge.connector.mongo.MongoChangeStreamSource;
import com.streamforge.core.config.ScopedConfig;
import com.streamforge.core.launcher.StreamJob;
import com.streamforge.core.pipeline.PipelineBuilder;
import com.streamforge.job.sync.cdc.parser.MongoToKafkaParser;
import com.streamforge.job.sync.cdc.processor.MongoToKafkaProcessor;
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
        .process(new MongoToKafkaProcessor())
        .to(new KafkaSinkBuilder(), name());

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
