package com.scalar.db.storage.cosmos;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedSet;
import com.scalar.db.util.ImmutableLinkedHashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A metadata class for a table of Scalar DB to know the type of each column
 *
 * @author Yuji Ito
 */
public class CosmosTableMetadata {
  private String id;
  private LinkedHashSet<String> partitionKeyNames;
  private LinkedHashSet<String> clusteringKeyNames;
  private Set<String> secondaryIndexNames;
  private Map<String, String> columns;

  public CosmosTableMetadata() {}

  public void setId(String id) {
    this.id = id;
  }

  public void setPartitionKeyNames(List<String> partitionKeyNames) {
    this.partitionKeyNames = new ImmutableLinkedHashSet<>(partitionKeyNames);
  }

  public void setClusteringKeyNames(List<String> clusteringKeyNames) {
    this.clusteringKeyNames = new ImmutableLinkedHashSet<>(clusteringKeyNames);
  }

  public void setSecondaryIndexNames(Set<String> secondaryIndexNames) {
    this.secondaryIndexNames = ImmutableSortedSet.copyOf(secondaryIndexNames);
  }

  public void setColumns(Map<String, String> columns) {
    this.columns = ImmutableMap.copyOf(columns);
  }

  public String getId() {
    return id;
  }

  public LinkedHashSet<String> getPartitionKeyNames() {
    return partitionKeyNames;
  }

  public LinkedHashSet<String> getClusteringKeyNames() {
    return clusteringKeyNames;
  }

  public Set<String> getSecondaryIndexNames() {
    return secondaryIndexNames;
  }

  public Map<String, String> getColumns() {
    return columns;
  }
}
