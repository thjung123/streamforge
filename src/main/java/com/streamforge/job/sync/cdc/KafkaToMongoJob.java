package com.streamforge.job.sync.cdc;

import com.streamforge.connector.kafka.KafkaSourceBuilder;
import com.streamforge.connector.mongo.MongoSinkBuilder;
import com.streamforge.core.config.ScopedConfig;
import com.streamforge.core.launcher.StreamJob;
import com.streamforge.core.pipeline.PipelineBuilder;
import com.streamforge.job.sync.cdc.parser.KafkaToMongoParser;
import com.streamforge.job.sync.cdc.processor.KafkaToMongoProcessor;
import com.streamforge.pattern.quality.ConstraintEnforcer;
import com.streamforge.pattern.quality.rules.NotNullRule;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class KafkaToMongoJob implements StreamJob {

  public static final String JOB_NAME = "KafkaToMongo";
  public static final String PARSER_NAME = "KafkaToMongoParser";
  public static final String PROCESSOR_NAME = "KafkaToMongoProcessor";

  @Override
  public String name() {
    return JOB_NAME;
  }

  public StreamExecutionEnvironment buildPipeline() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    PipelineBuilder.from(new KafkaSourceBuilder().build(env, name()))
        .parse(new KafkaToMongoParser())
        .apply(new ConstraintEnforcer<>(new NotNullRule("_id")))
        .process(new KafkaToMongoProcessor())
        .to(new MongoSinkBuilder(), name());
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

  /** Direct Flink cluster submission: flink run -c ...KafkaToMongoJob streamforge.jar */
  public static void main(String[] args) throws Exception {
    new KafkaToMongoJob().run(args);
  }
}
