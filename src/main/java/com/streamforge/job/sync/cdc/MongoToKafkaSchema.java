package com.streamforge.job.sync.cdc;

import com.streamforge.pattern.schema.SchemaVersion;
import com.streamforge.pattern.schema.SchemaVersion.FieldType;
import java.util.List;

public final class MongoToKafkaSchema {

  private MongoToKafkaSchema() {}

  public static final SchemaVersion V1 =
      SchemaVersion.builder().required("_id", FieldType.ANY).build();

  public static final List<SchemaVersion> VERSIONS = List.of(V1);
}
