package com.streamforge.job.sync.cdc;

import com.streamforge.connector.elasticsearch.ElasticsearchSinkBuilder;
import com.streamforge.connector.kafka.KafkaSourceBuilder;
import com.streamforge.connector.mongo.MongoSinkBuilder;
import com.streamforge.core.config.ScopedConfig;
import com.streamforge.core.launcher.StreamJob;
import com.streamforge.core.model.StreamEnvelop;
import com.streamforge.job.sync.cdc.parser.KafkaToMongoParser;
import com.streamforge.pattern.split.ParallelSplitter;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class EventRouterJob implements StreamJob {

  public static final String JOB_NAME = "EventRouter";

  @Override
  public String name() {
    return JOB_NAME;
  }

  public StreamExecutionEnvironment buildPipeline() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    DataStream<String> source = new KafkaSourceBuilder().build(env, name());
    DataStream<StreamEnvelop> parsed = new KafkaToMongoParser().parse(source);

    var splitter =
        ParallelSplitter.builder(TypeInformation.of(StreamEnvelop.class))
            .route("orders", e -> "orders".equals(e.getSource()))
            .route("payments", e -> "payments".equals(e.getSource()))
            .build();

    DataStream<StreamEnvelop> main = splitter.apply(parsed);
    DataStream<StreamEnvelop> orders = splitter.getSideOutput("orders");
    DataStream<StreamEnvelop> payments = splitter.getSideOutput("payments");

    ElasticsearchSinkBuilder esSink = new ElasticsearchSinkBuilder();
    esSink.write(orders, name());
    esSink.write(payments, name());

    MongoSinkBuilder mongoSink = new MongoSinkBuilder();
    mongoSink.write(main, name());

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
    new EventRouterJob().run(args);
  }
}
