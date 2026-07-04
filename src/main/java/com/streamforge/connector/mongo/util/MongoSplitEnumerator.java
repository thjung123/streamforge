package com.streamforge.connector.mongo.util;

import java.util.List;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;

public class MongoSplitEnumerator implements SplitEnumerator<MongoSplit, Boolean> {

  private final SplitEnumeratorContext<MongoSplit> context;
  private final int splitIndex;
  private final int numSplits;
  private boolean assigned;
  private MongoSplit returned;

  public MongoSplitEnumerator(
      SplitEnumeratorContext<MongoSplit> context, int splitIndex, int numSplits, boolean assigned) {
    this.context = context;
    this.splitIndex = splitIndex;
    this.numSplits = numSplits;
    this.assigned = assigned;
  }

  @Override
  public void start() {}

  @Override
  public void handleSplitRequest(int subtaskId, String requesterHostname) {
    assign(subtaskId);
  }

  @Override
  public void addReader(int subtaskId) {
    assign(subtaskId);
  }

  private void assign(int subtaskId) {
    if (assigned && returned == null) {
      return;
    }
    MongoSplit split = returned != null ? returned : new MongoSplit(splitIndex, numSplits, null);
    returned = null;
    context.assignSplit(split, subtaskId);
    context.signalNoMoreSplits(subtaskId);
    assigned = true;
  }

  @Override
  public void addSplitsBack(List<MongoSplit> splits, int subtaskId) {
    if (!splits.isEmpty()) {
      returned = splits.get(0);
      assigned = false;
    }
  }

  @Override
  public Boolean snapshotState(long checkpointId) {
    return assigned;
  }

  @Override
  public void close() {}
}
