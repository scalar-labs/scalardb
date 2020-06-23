package com.scalar.db.storage.cosmos;

import com.google.common.collect.ImmutableMap;
import java.util.Map;

public class Record {
  private String id;
  private String concatenatedPartitionKey;
  private Map<String, Object> partitionKey;
  private Map<String, Object> clusteringKey;
  private Map<String, Object> values;

  public Record() {}

  public void setId(String id) {
    this.id = id;
  }

  public void setConcatenatedPartitionKey(String concatenatedPartitionKey) {
    this.concatenatedPartitionKey = concatenatedPartitionKey;
  }

  public void setPartitionKey(Map<String, Object> partitionKey) {
    this.partitionKey = partitionKey;
  }

  public void setClusteringKey(Map<String, Object> clusteringKey) {
    this.clusteringKey = clusteringKey;
  }

  public void setValues(Map<String, Object> values) {
    this.values = values;
  }

  public String getId() {
    return id;
  }

  public String getConcatenatedPartitionKey() {
    return concatenatedPartitionKey;
  }

  public Map<String, Object> getPartitionKey() {
    return ImmutableMap.copyOf(partitionKey);
  }

  public Map<String, Object> getClusteringKey() {
    return ImmutableMap.copyOf(clusteringKey);
  }

  public Map<String, Object> getValues() {
    return ImmutableMap.copyOf(values);
  }
}
