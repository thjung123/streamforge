package com.streamforge.job.join;

import static com.streamforge.connector.kafka.KafkaConfigKeys.*;

import com.streamforge.connector.kafka.KafkaSourceBuilder;
import com.streamforge.connector.mongo.MongoSinkBuilder;
import com.streamforge.core.config.ScopedConfig;
import com.streamforge.core.launcher.StreamJob;
import com.streamforge.core.model.StreamEnvelop;
import com.streamforge.core.parser.StreamEnvelopParser;
import com.streamforge.core.pipeline.PipelineBuilder;
import com.streamforge.pattern.enrich.DynamicJoiner;
import java.time.Duration;
import java.util.HashMap;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class OrderPaymentJoinJob implements StreamJob {

  public static final String JOB_NAME = "OrderPaymentJoin";
  public static final String PAYMENT_TOPIC = "PAYMENT_TOPIC";
  public static final String JOIN_TTL_MINUTES = "JOIN_TTL_MINUTES";

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
    DataStream<String> ordersRaw = new KafkaSourceBuilder().build(env, name());
    DataStream<StreamEnvelop> orders = new StreamEnvelopParser(JOB_NAME).parse(ordersRaw);

    String paymentTopic = ScopedConfig.require(PAYMENT_TOPIC);
    DataStream<String> paymentsRaw =
        new KafkaSourceBuilder().build(env, name() + "-payments", paymentTopic);
    DataStream<StreamEnvelop> payments = new StreamEnvelopParser(JOB_NAME).parse(paymentsRaw);

    Duration ttl =
        Duration.ofMinutes(Long.parseLong(ScopedConfig.getOrDefault(JOIN_TTL_MINUTES, "10")));

    DataStream<StreamEnvelop> joined =
        PipelineBuilder.from(orders).enrich(payments, buildJoiner(ttl)).getStream();

    sinkBuilder.write(joined, name());

    return env;
  }

  static DynamicJoiner<StreamEnvelop, StreamEnvelop> buildJoiner(Duration ttl) {
    return DynamicJoiner.<StreamEnvelop, StreamEnvelop>builder()
        .leftKeyExtractor(StreamEnvelop::getPrimaryKey)
        .rightKeyExtractor(StreamEnvelop::getPrimaryKey)
        .joinFunction(
            (order, payment) -> {
              if (order.getMetadata() == null) {
                order.setMetadata(new HashMap<>());
              }
              order.getMetadata().put("paymentPayload", payment.getPayloadJson());
              order.getMetadata().put("paymentSource", payment.getSource());
              return order;
            })
        .joinType(DynamicJoiner.JoinType.LEFT)
        .ttl(ttl)
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
    new OrderPaymentJoinJob().run(args);
  }
}
