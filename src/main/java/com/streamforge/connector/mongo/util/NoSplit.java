package com.streamforge.connector.mongo.util;

import java.io.Serializable;
import org.apache.flink.api.connector.source.SourceSplit;

public class NoSplit implements SourceSplit, Serializable {
  public static final NoSplit INSTANCE = new NoSplit();

  NoSplit() {}

  @Override
  public String splitId() {
    return "nosplit";
  }

  @Override
  public String toString() {
    return "NoSplit";
  }
}
