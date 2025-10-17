package com.flinkcdc.common.source.util;

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

public class NoSplitEnumerator<T> implements SplitEnumerator<NoSplit, Void> {
    private final SplitEnumeratorContext<NoSplit> context;
    private boolean assigned = false;

    public NoSplitEnumerator(SplitEnumeratorContext<NoSplit> context) {
        this.context = context;
    }

    @Override
    public void start() {
    }

    @Override
    public void handleSplitRequest(int subtaskId, @org.checkerframework.checker.nullness.qual.Nullable String hostname) {
        if (!assigned) {
            context.assignSplits(
                    new SplitsAssignment<>(
                            Collections.singletonMap(subtaskId, Collections.singletonList(NoSplit.INSTANCE))
                    )
            );
            context.signalNoMoreSplits(subtaskId);
            assigned = true;
        }
    }

    @Override
    public void addSplitsBack(java.util.List<NoSplit> splits, int subtaskId) {
    }

    @Override
    public void addReader(int subtaskId) {
        if (!assigned) {
            context.assignSplits(
                    new SplitsAssignment<>(
                            Collections.singletonMap(subtaskId, Collections.singletonList(NoSplit.INSTANCE))
                    )
            );
            context.signalNoMoreSplits(subtaskId);
            assigned = true;
        }
    }

    @Override
    public Void snapshotState(long checkpointId) throws Exception {
        return null;
    }

    @Override
    public void close() throws IOException {
    }
}
