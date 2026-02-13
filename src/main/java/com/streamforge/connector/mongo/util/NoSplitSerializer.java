package com.streamforge.connector.mongo.util;

import org.apache.flink.core.io.SimpleVersionedSerializer;

public class NoSplitSerializer implements SimpleVersionedSerializer<NoSplit> {

    public static final NoSplitSerializer INSTANCE = new NoSplitSerializer();

    @Override
    public int getVersion() {
        return 1;
    }

    @Override
    public byte[] serialize(NoSplit split) {
        return new byte[0];
    }

    @Override
    public NoSplit deserialize(int version, byte[] serialized) {
        return new NoSplit();
    }
}
