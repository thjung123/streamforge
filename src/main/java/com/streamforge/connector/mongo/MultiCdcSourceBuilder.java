package com.streamforge.connector.mongo;

import static com.streamforge.core.config.ScopedConfig.getOrDefault;

import com.streamforge.core.pipeline.PipelineBuilder;
import java.io.Serial;
import java.io.Serializable;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.bson.Document;

public class MultiCdcSourceBuilder
    implements PipelineBuilder.SourceBuilder<Document>, Serializable {

  @Serial private static final long serialVersionUID = 1L;

  public static final String CDC_PARALLELISM = "CDC_PARALLELISM";
  private static final int DEFAULT_PARALLELISM = 4;

  @Override
  public DataStream<Document> build(StreamExecutionEnvironment env, String jobName) {
    int numConnectors =
        Integer.parseInt(getOrDefault(CDC_PARALLELISM, String.valueOf(DEFAULT_PARALLELISM)));
    return build(env, jobName, numConnectors);
  }

  @SuppressWarnings("unchecked")
  DataStream<Document> build(StreamExecutionEnvironment env, String jobName, int numConnectors) {
    if (numConnectors <= 1) {
      return new MongoChangeStreamSource().build(env, jobName);
    }

    MongoChangeStreamSource source = new MongoChangeStreamSource();
    DataStream<Document> first = source.build(env, jobName, 0, numConnectors);

    DataStream<Document>[] rest = new DataStream[numConnectors - 1];
    for (int i = 1; i < numConnectors; i++) {
      rest[i - 1] = source.build(env, jobName, i, numConnectors);
    }

    return first.union(rest);
  }
}
