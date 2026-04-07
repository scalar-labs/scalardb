package com.scalar.db.storage.objectstorage;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.fury.Fury;
import org.apache.fury.ThreadSafeFury;
import org.apache.fury.config.Language;
import org.apache.fury.memory.MemoryBuffer;

public class ForyDataSerializer implements ObjectStorageDataSerializer {
  private static final ThreadSafeFury fury;

  static {
    fury =
        Fury.builder()
            .withLanguage(Language.JAVA)
            .requireClassRegistration(false)
            .buildThreadSafeFury();
    fury.register(ObjectStoragePartition.class);
    fury.register(ObjectStorageRecord.class);
    fury.register(HashMap.class);
    fury.register(Map.class);
  }

  @Nullable private final ObjectStorageTableMetadata metadata;

  public ForyDataSerializer(@Nullable ObjectStorageTableMetadata metadata) {
    this.metadata = metadata;
  }

  @Override
  public byte[] serialize(ObjectStoragePartition partition) {
    try {
      int estimatedSize = estimateOutputSize(partition, metadata);
      MemoryBuffer buffer = MemoryBuffer.newHeapBuffer(estimatedSize);
      fury.serialize(buffer, partition);
      return buffer.getBytes(0, buffer.writerIndex());
    } catch (Exception e) {
      throw new RuntimeException("Failed to serialize the partition with Fory.", e);
    }
  }

  @Override
  public ObjectStoragePartition deserialize(byte[] data) {
    try {
      return (ObjectStoragePartition) fury.deserialize(data);
    } catch (Exception e) {
      throw new RuntimeException("Failed to deserialize the partition with Fory.", e);
    }
  }

  private static int estimateOutputSize(
      ObjectStoragePartition partition, @Nullable ObjectStorageTableMetadata metadata) {
    int recordCount = partition.getRecords().size();
    if (recordCount == 0) {
      return 1024;
    }

    long bytesPerRecord;
    if (metadata != null) {
      bytesPerRecord = 0;
      for (String columnType : metadata.getColumns().values()) {
        switch (columnType) {
          case "boolean":
            bytesPerRecord += 1;
            break;
          case "int":
          case "float":
          case "date":
            bytesPerRecord += 4;
            break;
          case "bigint":
          case "double":
          case "time":
          case "timestamp":
          case "timestamptz":
            bytesPerRecord += 8;
            break;
          case "text":
            bytesPerRecord += 64;
            break;
          case "blob":
            bytesPerRecord += 1024;
            break;
          default:
            bytesPerRecord += 32;
            break;
        }
      }
    } else {
      // Fallback: use the default internal buffer when metadata is unavailable
      return 64;
    }

    long estimated = (long) recordCount * bytesPerRecord;
    return (int) Math.min(estimated, Integer.MAX_VALUE - 8);
  }
}
