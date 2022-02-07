package com.scalar.db.storage.cosmos;

import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * A record class that Cosmos DB uses for storing a document based on Scalar DB data model.
 *
 * @author Yuji Ito
 */
@NotThreadSafe
public class Record {
  private String id = "";
  private String concatenatedPartitionKey = "";
  private Map<String, Object> partitionKey = Collections.emptyMap();
  private Map<String, Object> clusteringKey = Collections.emptyMap();
  private Map<String, Object> values = Collections.emptyMap();

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
    return other.getValues().equals(values);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, concatenatedPartitionKey, partitionKey, clusteringKey, values);
  }
}
