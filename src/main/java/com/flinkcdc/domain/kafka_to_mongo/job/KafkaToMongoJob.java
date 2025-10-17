package com.flinkcdc.domain.kafka_to_mongo.job;

import com.flinkcdc.common.launcher.FlinkJob;
import com.flinkcdc.common.pipeline.PipelineBuilder;
import com.flinkcdc.common.sink.MongoSinkBuilder;
import com.flinkcdc.common.source.KafkaSourceBuilder;
import com.flinkcdc.domain.kafka_to_mongo.MongoToKafkaConstants;
import com.flinkcdc.domain.kafka_to_mongo.parser.KafkaToMongoParser;
import com.flinkcdc.domain.kafka_to_mongo.processor.KafkaToMongoProcessor;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class KafkaToMongoJob implements FlinkJob {

    @Override
    public String name() {
        return MongoToKafkaConstants.JOB_NAME;
    }

    public StreamExecutionEnvironment buildPipeline() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        PipelineBuilder
                .from(new KafkaSourceBuilder().build(env, name()))
                .parse(new KafkaToMongoParser())
                .process(new KafkaToMongoProcessor())
                .to(new MongoSinkBuilder(), name());
        return env;
    }

    @Override
    public void run(String[] args) throws Exception {
        StreamExecutionEnvironment env = buildPipeline();
        JobExecutionResult result = env.execute(name());
        System.out.println("Job duration: " + result.getNetRuntime());
    }
}
