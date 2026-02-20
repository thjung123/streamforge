package com.streamforge.pattern.split;

import com.streamforge.core.config.MetricKeys;
import com.streamforge.core.metric.Metrics;
import com.streamforge.core.pipeline.PipelineBuilder;
import com.streamforge.pattern.filter.SerializablePredicate;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class ParallelSplitter<T> implements PipelineBuilder.StreamPattern<T> {

  private final List<Route<T>> routes;
  private final boolean copyToMain;
  private transient SingleOutputStreamOperator<T> result;

  private ParallelSplitter(List<Route<T>> routes, boolean copyToMain) {
    this.routes = new ArrayList<>(routes);
    this.copyToMain = copyToMain;
  }

  public static <T> Builder<T> builder(TypeInformation<T> typeInfo) {
    return new Builder<>(typeInfo);
  }

  @Override
  public DataStream<T> apply(DataStream<T> stream) {
    this.result = stream.process(new SplitFunction<>(routes, copyToMain, name())).name(name());
    return result;
  }

  public DataStream<T> getSideOutput(String routeName) {
    if (result == null) {
      throw new IllegalStateException("apply() must be called before getSideOutput()");
    }
    return result.getSideOutput(tag(routeName));
  }

  public OutputTag<T> tag(String routeName) {
    return routes.stream()
        .filter(r -> r.getName().equals(routeName))
        .findFirst()
        .map(Route::getOutputTag)
        .orElseThrow(() -> new IllegalArgumentException("Unknown route: " + routeName));
  }

  public static final class Builder<T> {

    private final TypeInformation<T> typeInfo;
    private final Map<String, Route<T>> routes = new LinkedHashMap<>();
    private boolean copyToMain;

    private Builder(TypeInformation<T> typeInfo) {
      this.typeInfo = typeInfo;
    }

    public Builder<T> route(String name, SerializablePredicate<T> predicate) {
      if (name == null || name.isBlank()) {
        throw new IllegalArgumentException("Route name must not be null or blank");
      }
      if (predicate == null) {
        throw new IllegalArgumentException("Route predicate must not be null");
      }
      OutputTag<T> tag = new OutputTag<>(name, typeInfo);
      routes.put(name, new Route<>(name, predicate, tag));
      return this;
    }

    public Builder<T> copyToMain(boolean copyToMain) {
      this.copyToMain = copyToMain;
      return this;
    }

    public ParallelSplitter<T> build() {
      if (routes.isEmpty()) {
        throw new IllegalArgumentException("At least one route must be defined");
      }
      return new ParallelSplitter<>(new ArrayList<>(routes.values()), copyToMain);
    }
  }

  static class SplitFunction<T> extends ProcessFunction<T, T> {

    private final List<Route<T>> routes;
    private final boolean copyToMain;
    private final String operatorName;
    private transient Metrics metrics;

    SplitFunction(List<Route<T>> routes, boolean copyToMain, String operatorName) {
      this.routes = new ArrayList<>(routes);
      this.copyToMain = copyToMain;
      this.operatorName = operatorName;
    }

    @Override
    public void open(Configuration parameters) {
      this.metrics = new Metrics(getRuntimeContext(), "splitter", operatorName);
    }

    @Override
    public void processElement(T value, Context ctx, Collector<T> out) {
      if (copyToMain) {
        out.collect(value);
        for (Route<T> route : routes) {
          if (route.getPredicate().test(value)) {
            ctx.output(route.getOutputTag(), value);
            metrics.inc("splitter.route." + route.getName() + "_count");
          }
        }
      } else {
        boolean matched = false;
        for (Route<T> route : routes) {
          if (route.getPredicate().test(value)) {
            ctx.output(route.getOutputTag(), value);
            metrics.inc("splitter.route." + route.getName() + "_count");
            matched = true;
            break;
          }
        }
        if (!matched) {
          out.collect(value);
          metrics.inc(MetricKeys.SPLITTER_UNMATCHED_COUNT);
        }
      }
    }
  }
}
