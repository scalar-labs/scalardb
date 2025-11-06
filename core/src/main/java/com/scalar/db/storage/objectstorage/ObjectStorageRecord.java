package com.scalar.db.storage.objectstorage;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

@SuppressFBWarnings({"EI_EXPOSE_REP", "EI_EXPOSE_REP2"})
@Immutable
public class ObjectStorageRecord {
  private final String id;
  private final Map<String, Object> partitionKey;
  private final Map<String, Object> clusteringKey;
  private final Map<String, Object> values;

  @JsonCreator
  public ObjectStorageRecord(
      @JsonProperty("id") @Nullable String id,
      @JsonProperty("partitionKey") @Nullable Map<String, Object> partitionKey,
      @JsonProperty("clusteringKey") @Nullable Map<String, Object> clusteringKey,
      @JsonProperty("values") @Nullable Map<String, Object> values) {
    this.id = id != null ? id : "";
    this.partitionKey = partitionKey != null ? new HashMap<>(partitionKey) : Collections.emptyMap();
    this.clusteringKey =
        clusteringKey != null ? new HashMap<>(clusteringKey) : Collections.emptyMap();
    this.values = values != null ? new HashMap<>(values) : Collections.emptyMap();
  }

  public ObjectStorageRecord(ObjectStorageRecord record) {
    this(record.getId(), record.getPartitionKey(), record.getClusteringKey(), record.getValues());
  }

  public String getId() {
    return id;
  }

  public Map<String, Object> getPartitionKey() {
    return Collections.unmodifiableMap(partitionKey);
  }

  public Map<String, Object> getClusteringKey() {
    return Collections.unmodifiableMap(clusteringKey);
  }

  public Map<String, Object> getValues() {
    return Collections.unmodifiableMap(values);
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
    if (!other.getId().equals(id)) {
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
    return Objects.hash(id, partitionKey, clusteringKey, values);
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static final class Builder {
    private String id;
    private Map<String, Object> partitionKey = new HashMap<>();
    private Map<String, Object> clusteringKey = new HashMap<>();
    private Map<String, Object> values = new HashMap<>();

    public Builder id(String id) {
      this.id = id;
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
      return new ObjectStorageRecord(id, partitionKey, clusteringKey, values);
    }
  }
}
