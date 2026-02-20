package com.streamforge.job.cdc;

import com.streamforge.connector.kafka.KafkaSourceBuilder;
import com.streamforge.connector.mongo.MongoSinkBuilder;
import com.streamforge.core.config.ScopedConfig;
import com.streamforge.core.launcher.StreamJob;
import com.streamforge.core.model.StreamEnvelop;
import com.streamforge.core.pipeline.PipelineBuilder;
import com.streamforge.job.cdc.parser.KafkaToMongoParser;
import com.streamforge.job.cdc.processor.KafkaToMongoProcessor;
import com.streamforge.pattern.enrich.StaticJoiner;
import com.streamforge.pattern.quality.ConstraintEnforcer;
import com.streamforge.pattern.quality.rules.NotNullRule;
import java.util.HashMap;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class KafkaToMongoJob implements StreamJob {

  public static final String JOB_NAME = "KafkaToMongo";
  public static final String PARSER_NAME = "KafkaToMongoParser";
  public static final String PROCESSOR_NAME = "KafkaToMongoProcessor";
  public static final String REFERENCE_TOPIC = "REFERENCE_TOPIC";
  public static final String REFERENCE_TOPIC_2 = "REFERENCE_TOPIC_2";

  @Override
  public String name() {
    return JOB_NAME;
  }

  public StreamExecutionEnvironment buildPipeline() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    DataStream<StreamEnvelop> ref1 = buildReferenceStream(env, REFERENCE_TOPIC, "ref");
    DataStream<StreamEnvelop> ref2 = buildReferenceStream(env, REFERENCE_TOPIC_2, "ref2");

    var builder =
        PipelineBuilder.from(new KafkaSourceBuilder().build(env, name()))
            .parse(new KafkaToMongoParser());

    if (ref1 != null) {
      builder = builder.enrich(ref1, buildJoiner("ref-state-1", "enrichedRef1"));
    }
    if (ref2 != null) {
      builder = builder.enrich(ref2, buildJoiner("ref-state-2", "enrichedRef2"));
    }

    builder
        .apply(new ConstraintEnforcer<>(new NotNullRule("_id")))
        .process(new KafkaToMongoProcessor())
        .to(new MongoSinkBuilder(), name());

    return env;
  }

  private DataStream<StreamEnvelop> buildReferenceStream(
      StreamExecutionEnvironment env, String topicKey, String suffix) {
    if (!ScopedConfig.exists(topicKey)) {
      return null;
    }
    String refTopic = ScopedConfig.require(topicKey);
    DataStream<String> refRaw =
        new KafkaSourceBuilder().build(env, name() + "-" + suffix, refTopic);
    return new KafkaToMongoParser().parse(refRaw);
  }

  private StaticJoiner<StreamEnvelop, StreamEnvelop> buildJoiner(
      String stateName, String metadataKey) {
    MapStateDescriptor<String, StreamEnvelop> descriptor =
        new MapStateDescriptor<>(stateName, Types.STRING, TypeInformation.of(StreamEnvelop.class));
    return StaticJoiner.<StreamEnvelop, StreamEnvelop>builder()
        .mainKeyExtractor(StreamEnvelop::getPrimaryKey)
        .refKeyExtractor(StreamEnvelop::getPrimaryKey)
        .joinFunction(
            (event, refData) -> {
              if (event.getMetadata() == null) {
                event.setMetadata(new HashMap<>());
              }
              event.getMetadata().put(metadataKey, refData.getPayloadJson());
              return event;
            })
        .descriptor(descriptor)
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

  /** Direct Flink cluster submission: flink run -c ...KafkaToMongoJob streamforge.jar */
  public static void main(String[] args) throws Exception {
    new KafkaToMongoJob().run(args);
  }
}
