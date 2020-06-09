package com.scalar.db.storage.cosmos;

import java.util.Collections;
import java.util.Map;

public class Record {
  private String id;
  private String concatPartitionKey;
  private Map<String, Object> partitionKey;
  private Map<String, Object> clusteringKey;
  private Map<String, Object> values;

  public Record() {}

  public void setId(String id) {
    this.id = id;
  }

  public void setConcatPartitionKey(String concatPartitionKey) {
    this.concatPartitionKey = concatPartitionKey;
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

  public String getConcatPartitionKey() {
    return concatPartitionKey;
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
}
