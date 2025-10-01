package com.flinkcdc.domain.sample1.job;

import com.flinkcdc.common.launcher.FlinkJob;
import com.flinkcdc.common.pipeline.PipelineBuilder;
import com.flinkcdc.common.sink.KafkaSinkBuilder;
import com.flinkcdc.common.source.MongoSourceBuilder;
import com.flinkcdc.domain.sample1.Sample1Constants;
import com.flinkcdc.domain.sample1.parser.Sample1Parser;
import com.flinkcdc.domain.sample1.processor.Sample1Processor;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Sample1Job implements FlinkJob {

    @Override
    public String name() {
        return Sample1Constants.JOB_NAME;
    }

    private StreamExecutionEnvironment buildPipeline() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        PipelineBuilder
                .from(new MongoSourceBuilder().build(env, name()))
                .parse(new Sample1Parser())
                .process(new Sample1Processor())
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
