package com.streamforge.connector.elasticsearch;

import static com.streamforge.connector.elasticsearch.ElasticsearchConfigKeys.*;
import static com.streamforge.core.config.ScopedConfig.*;

import com.streamforge.core.model.StreamEnvelop;
import com.streamforge.core.pipeline.PipelineBuilder;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.connector.elasticsearch.sink.Elasticsearch7SinkBuilder;
import org.apache.flink.connector.elasticsearch.sink.ElasticsearchEmitter;
import org.apache.flink.connector.elasticsearch.sink.RequestIndexer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElasticsearchSinkBuilder implements PipelineBuilder.SinkBuilder<StreamEnvelop> {

  public static final String OPERATOR_NAME = "ElasticsearchSink";

  @Override
  public DataStreamSink<StreamEnvelop> write(DataStream<StreamEnvelop> stream, String jobName) {
    String host = require(ES_HOST);
    String index = require(ES_INDEX);

    var sink =
        new Elasticsearch7SinkBuilder<StreamEnvelop>()
            .setHosts(HttpHost.create(host))
            .setEmitter(new StreamEnvelopEmitter(index))
            .setBulkFlushMaxActions(1)
            .build();

    return stream.sinkTo(sink).name(OPERATOR_NAME);
  }

  static class StreamEnvelopEmitter implements ElasticsearchEmitter<StreamEnvelop> {

    private static final Logger log = LoggerFactory.getLogger(StreamEnvelopEmitter.class);
    private final String index;

    StreamEnvelopEmitter(String index) {
      this.index = index;
    }

    @Override
    public void emit(StreamEnvelop element, SinkWriter.Context context, RequestIndexer indexer) {
      String docId = element.getTraceId();
      if (docId == null || docId.isBlank()) {
        log.warn("[EsSink] Missing traceId, skipping: {}", element);
        return;
      }
      indexer.add(new IndexRequest(index).id(docId).source(element.toJson(), XContentType.JSON));
    }
  }
}
