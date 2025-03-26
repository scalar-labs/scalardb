package com.scalar.db.storage.objectstorage;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

@Immutable
public class ObjectStorageRecord {
  private final String concatenatedKey;
  private final Map<String, Object> partitionKey;
  private final Map<String, Object> clusteringKey;
  private final Map<String, Object> values;

  // The default constructor is required by Jackson to deserialize JSON object
  public ObjectStorageRecord() {
    this(null, null, null, null);
  }

  public ObjectStorageRecord(
      @Nullable String concatenatedKey,
      @Nullable Map<String, Object> partitionKey,
      @Nullable Map<String, Object> clusteringKey,
      @Nullable Map<String, Object> values) {
    this.concatenatedKey = concatenatedKey != null ? concatenatedKey : "";
    this.partitionKey = partitionKey != null ? partitionKey : Collections.emptyMap();
    this.clusteringKey = clusteringKey != null ? clusteringKey : Collections.emptyMap();
    this.values = values != null ? values : Collections.emptyMap();
  }

  public ObjectStorageRecord(ObjectStorageRecord record) {
    this(
        record.getConcatenatedKey(),
        record.getPartitionKey(),
        record.getClusteringKey(),
        record.getValues());
  }

  public String getConcatenatedKey() {
    return concatenatedKey;
  }

  public Map<String, Object> getPartitionKey() {
    return partitionKey;
  }

  public Map<String, Object> getClusteringKey() {
    return clusteringKey;
  }

  public Map<String, Object> getValues() {
    return values;
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (!(o instanceof ObjectStorageRecord)) {
      return false;
    }
    ObjectStorageRecord other = (ObjectStorageRecord) o;
    if (!other.getConcatenatedKey().equals(concatenatedKey)) {
      return false;
    }
    if (!other.getPartitionKey().equals(partitionKey)) {
      return false;
    }
    if (!other.getClusteringKey().equals(clusteringKey)) {
      return false;
    }
    return other.getValues().equals(values);
  }

  @Override
  public int hashCode() {
    return Objects.hash(concatenatedKey, partitionKey, clusteringKey, values);
  }

  // Builder

  public static class Builder {
    private String concatenatedPartitionKey;
    private Map<String, Object> partitionKey;
    private Map<String, Object> clusteringKey;
    private Map<String, Object> values;

    public Builder() {}

    public Builder concatenatedPartitionKey(String concatenatedPartitionKey) {
      this.concatenatedPartitionKey = concatenatedPartitionKey;
      return this;
    }

    public Builder partitionKey(Map<String, Object> partitionKey) {
      this.partitionKey = partitionKey;
      return this;
    }

    public Builder clusteringKey(Map<String, Object> clusteringKey) {
      this.clusteringKey = clusteringKey;
      return this;
    }

    public Builder values(Map<String, Object> values) {
      this.values = values;
      return this;
    }

    public ObjectStorageRecord build() {
      return new ObjectStorageRecord(concatenatedPartitionKey, partitionKey, clusteringKey, values);
    }
  }
}
