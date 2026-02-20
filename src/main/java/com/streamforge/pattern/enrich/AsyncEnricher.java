package com.streamforge.pattern.enrich;

import com.streamforge.core.config.MetricKeys;
import com.streamforge.core.metric.Metrics;
import com.streamforge.core.pipeline.PipelineBuilder;
import java.io.Serializable;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AsyncEnricher<T> implements PipelineBuilder.StreamPattern<T> {

  public enum OrderingGuarantee {
    ORDERED,
    UNORDERED
  }

  @FunctionalInterface
  public interface AsyncLookupFunction<T> extends Serializable {
    CompletableFuture<T> lookup(T input);
  }

  private final AsyncLookupFunction<T> lookupFunction;
  private final Duration timeout;
  private final int capacity;
  private final OrderingGuarantee ordering;

  private AsyncEnricher(
      AsyncLookupFunction<T> lookupFunction,
      Duration timeout,
      int capacity,
      OrderingGuarantee ordering) {
    this.lookupFunction = lookupFunction;
    this.timeout = timeout;
    this.capacity = capacity;
    this.ordering = ordering;
  }

  public static <T> Builder<T> builder() {
    return new Builder<>();
  }

  @Override
  public DataStream<T> apply(DataStream<T> stream) {
    var asyncFunction = new EnrichAsyncFunction<>(lookupFunction);

    SingleOutputStreamOperator<T> result;
    if (ordering == OrderingGuarantee.ORDERED) {
      result =
          AsyncDataStream.orderedWait(
              stream, asyncFunction, timeout.toMillis(), TimeUnit.MILLISECONDS, capacity);
    } else {
      result =
          AsyncDataStream.unorderedWait(
              stream, asyncFunction, timeout.toMillis(), TimeUnit.MILLISECONDS, capacity);
    }

    return result.name("AsyncEnricher");
  }

  public static final class Builder<T> {

    private AsyncLookupFunction<T> lookupFunction;
    private Duration timeout = Duration.ofSeconds(30);
    private int capacity = 100;
    private OrderingGuarantee ordering = OrderingGuarantee.UNORDERED;

    private Builder() {}

    public Builder<T> lookupFunction(AsyncLookupFunction<T> lookupFunction) {
      this.lookupFunction = lookupFunction;
      return this;
    }

    public Builder<T> timeout(Duration timeout) {
      this.timeout = timeout;
      return this;
    }

    public Builder<T> capacity(int capacity) {
      this.capacity = capacity;
      return this;
    }

    public Builder<T> ordering(OrderingGuarantee ordering) {
      this.ordering = ordering;
      return this;
    }

    public AsyncEnricher<T> build() {
      if (lookupFunction == null) {
        throw new IllegalArgumentException("lookupFunction must not be null");
      }
      if (timeout == null) {
        throw new IllegalArgumentException("timeout must not be null");
      }
      if (capacity <= 0) {
        throw new IllegalArgumentException("capacity must be positive");
      }
      if (ordering == null) {
        throw new IllegalArgumentException("ordering must not be null");
      }
      return new AsyncEnricher<>(lookupFunction, timeout, capacity, ordering);
    }
  }

  static class EnrichAsyncFunction<T> extends RichAsyncFunction<T, T> {

    private static final Logger log = LoggerFactory.getLogger(AsyncEnricher.class);

    private final AsyncLookupFunction<T> lookupFunction;
    private transient Metrics metrics;

    EnrichAsyncFunction(AsyncLookupFunction<T> lookupFunction) {
      this.lookupFunction = lookupFunction;
    }

    @Override
    public void open(Configuration parameters) {
      this.metrics = new Metrics(getRuntimeContext(), "async_enricher", "AsyncEnricher");
    }

    @Override
    @SuppressWarnings("FutureReturnValueIgnored")
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) {
      lookupFunction
          .lookup(input)
          .whenComplete(
              (enriched, throwable) -> {
                if (throwable != null) {
                  log.warn("[AsyncEnricher] Lookup failed, passing through original", throwable);
                  metrics.inc(MetricKeys.ASYNC_ENRICHER_ERROR_COUNT);
                  resultFuture.complete(Collections.singleton(input));
                } else {
                  metrics.inc(MetricKeys.ASYNC_ENRICHER_SUCCESS_COUNT);
                  resultFuture.complete(Collections.singleton(enriched));
                }
              });
    }

    @Override
    public void timeout(T input, ResultFuture<T> resultFuture) {
      log.warn("[AsyncEnricher] Lookup timed out, passing through original");
      metrics.inc(MetricKeys.ASYNC_ENRICHER_TIMEOUT_COUNT);
      resultFuture.complete(Collections.singleton(input));
    }
  }
}
