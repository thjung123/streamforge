package com.streamforge.connector.mongo.util;

import java.io.Serializable;
import org.apache.flink.api.connector.source.SourceSplit;

public class MongoSplit implements SourceSplit, Serializable {

  private final int splitIndex;
  private final int numSplits;
  private final String resumeToken;

  public MongoSplit(int splitIndex, int numSplits, String resumeToken) {
    this.splitIndex = splitIndex;
    this.numSplits = numSplits;
    this.resumeToken = resumeToken;
  }

  public int splitIndex() {
    return splitIndex;
  }

  public int numSplits() {
    return numSplits;
  }

  public String resumeToken() {
    return resumeToken;
  }

  @Override
  public String splitId() {
    return "mongo-cdc-" + splitIndex + "-of-" + numSplits;
  }

  @Override
  public String toString() {
    return "MongoSplit{" + splitId() + ", resumed=" + (resumeToken != null) + "}";
  }
}
