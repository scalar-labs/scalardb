package com.scalar.db.storage.cosmos;

import com.google.common.collect.ImmutableMap;
import java.util.Map;

/**
 * A record class to present a row of Scalar DB as a document on Cosmos DB
 *
 * @author Yuji Ito
 */
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

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (!(o instanceof Record)) {
      return false;
    }

    Record other = (Record) o;
    if (!other.getConcatenatedPartitionKey().equals(concatenatedPartitionKey)) {
      return false;
    }
    if (!other.getId().equals(id)) {
      return false;
    }
    if (!other.getPartitionKey().equals(partitionKey)) {
      return false;
    }
    if (!other.getClusteringKey().equals(clusteringKey)) {
      return false;
    }
    if (!other.getValues().equals(values)) {
      return false;
    }

    return true;
  }
}
