package com.flinkcdc.domain.sample2.job;

import com.flinkcdc.common.launcher.FlinkJob;
import com.flinkcdc.common.pipeline.PipelineBuilder;
import com.flinkcdc.common.sink.MongoSinkBuilder;
import com.flinkcdc.common.source.KafkaSourceBuilder;
import com.flinkcdc.domain.sample2.Sample2Constants;
import com.flinkcdc.domain.sample2.parser.Sample2Parser;
import com.flinkcdc.domain.sample2.processor.Sample2Processor;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class Sample2Job implements FlinkJob {

    @Override
    public String name() {
        return Sample2Constants.JOB_NAME;
    }

    public StreamExecutionEnvironment buildPipeline() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        PipelineBuilder
                .from(new KafkaSourceBuilder().build(env, name()))
                .parse(new Sample2Parser())
                .process(new Sample2Processor())
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
