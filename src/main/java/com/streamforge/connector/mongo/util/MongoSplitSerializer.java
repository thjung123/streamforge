package com.streamforge.connector.mongo.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import org.apache.flink.core.io.SimpleVersionedSerializer;

public class MongoSplitSerializer implements SimpleVersionedSerializer<MongoSplit> {

  public static final MongoSplitSerializer INSTANCE = new MongoSplitSerializer();

  @Override
  public int getVersion() {
    return 1;
  }

  @Override
  public byte[] serialize(MongoSplit split) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (DataOutputStream out = new DataOutputStream(baos)) {
      out.writeInt(split.splitIndex());
      out.writeInt(split.numSplits());
      String token = split.resumeToken();
      out.writeBoolean(token != null);
      if (token != null) {
        out.writeUTF(token);
      }
    }
    return baos.toByteArray();
  }

  @Override
  public MongoSplit deserialize(int version, byte[] serialized) throws IOException {
    try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(serialized))) {
      int splitIndex = in.readInt();
      int numSplits = in.readInt();
      String token = in.readBoolean() ? in.readUTF() : null;
      return new MongoSplit(splitIndex, numSplits, token);
    }
  }
}
