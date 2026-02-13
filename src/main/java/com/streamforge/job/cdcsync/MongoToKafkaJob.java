package com.streamforge.job.cdcsync;

import com.streamforge.core.launcher.StreamJob;
import com.streamforge.core.pipeline.PipelineBuilder;
import com.streamforge.connector.kafka.KafkaSinkBuilder;
import com.streamforge.connector.mongo.CustomMongoCdcSource;
import com.streamforge.job.cdcsync.parser.MongoToKafkaParser;
import com.streamforge.job.cdcsync.processor.MongoToKafkaProcessor;
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

    /** Direct Flink cluster submission: flink run -c ...MongoToKafkaJob streamforge.jar */
    public static void main(String[] args) throws Exception {
        new MongoToKafkaJob().run(args);
    }
}
