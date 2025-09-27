package com.flinkcdc.domain.sample2.job;

import com.flinkcdc.common.launcher.FlinkJob;
import com.flinkcdc.common.pipeline.PipelineBuilder;
import com.flinkcdc.common.sink.KafkaSinkBuilder;
import com.flinkcdc.common.sink.MongoSinkBuilder;
import com.flinkcdc.common.source.KafkaSourceBuilder;
import com.flinkcdc.common.source.MongoSourceBuilder;
import com.flinkcdc.domain.sample1.processor.Sample1Processor;
import com.flinkcdc.domain.sample2.parser.Sample2Parser;
import com.flinkcdc.domain.sample2.processor.Sample2Processor;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
public class Sample2Job implements FlinkJob {

    @Override
    public String name() {
        return "sample2";
    }

    @Override
    public void run(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        JobExecutionResult result = PipelineBuilder
                .from(new KafkaSourceBuilder().build(env))
                .parse(new Sample2Parser())
                .process(new Sample2Processor())
                .to(new MongoSinkBuilder())
                .run("SampleJob2");

        System.out.println("Job duration: " + result.getNetRuntime());
    }
}
