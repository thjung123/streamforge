package com.streamforge.pattern.split;

import com.streamforge.pattern.filter.SerializablePredicate;
import java.io.Serial;
import java.io.Serializable;
import lombok.Getter;
import org.apache.flink.util.OutputTag;

@Getter
public class Route<T> implements Serializable {

  @Serial private static final long serialVersionUID = 1L;

  private final String name;
  private final SerializablePredicate<T> predicate;
  private final OutputTag<T> outputTag;

  Route(String name, SerializablePredicate<T> predicate, OutputTag<T> outputTag) {
    this.name = name;
    this.predicate = predicate;
    this.outputTag = outputTag;
  }
}
