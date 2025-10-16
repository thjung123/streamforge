package com.flinkcdc.domain.mongo_to_kafka.job;

import com.flinkcdc.common.launcher.FlinkJob;
import com.flinkcdc.common.pipeline.PipelineBuilder;
import com.flinkcdc.common.sink.KafkaSinkBuilder;
import com.flinkcdc.common.source.CustomMongoCdcSource;
import com.flinkcdc.domain.mongo_to_kafka.MongoToKafkaConstants;
import com.flinkcdc.domain.mongo_to_kafka.parser.MongoToKafkaParser;
import com.flinkcdc.domain.mongo_to_kafka.processor.MongoToKafkaProcessor;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MongoToKafkaJob implements FlinkJob {

    @Override
    public String name() {
        return MongoToKafkaConstants.JOB_NAME;
    }

    private StreamExecutionEnvironment buildPipeline() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        PipelineBuilder
                .from(new CustomMongoCdcSource().build(env, name()))
                .parse(new MongoToKafkaParser())
                .process(new MongoToKafkaProcessor())
                .to(new KafkaSinkBuilder(), name());

        return env;
    }

    @Override
    public void run(String[] args) throws Exception {
        StreamExecutionEnvironment env = buildPipeline();
        JobExecutionResult result = env.execute(name());
        System.out.println("Job duration: " + result.getNetRuntime());
    }
}
