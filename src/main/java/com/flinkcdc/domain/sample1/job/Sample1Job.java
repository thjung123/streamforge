package com.flinkcdc.domain.sample1.job;

import com.flinkcdc.common.launcher.FlinkJob;
import com.flinkcdc.common.pipeline.PipelineBuilder;
import com.flinkcdc.common.source.KafkaSourceBuilder;
import com.flinkcdc.common.sink.KafkaSinkBuilder;
import com.flinkcdc.domain.sample1.parser.Sample1Parser;
import com.flinkcdc.domain.sample1.processor.Sample1Processor;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Sample1Job implements FlinkJob {

    @Override
    public String name() {
        return "sample1";
    }

    @Override
    public void run(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        JobExecutionResult result = PipelineBuilder
                .from(new KafkaSourceBuilder().build(env))
                .parse(new Sample1Parser())
                .process(new Sample1Processor())
                .to(new KafkaSinkBuilder())
                .run("SampleJob1");
        System.out.println("Job duration: " + result.getNetRuntime());
    }
}
