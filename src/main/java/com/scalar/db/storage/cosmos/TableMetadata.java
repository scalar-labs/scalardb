package com.scalar.db.storage.cosmos;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

public class TableMetadata {
  private String id;
  private SortedSet<String> partitionKeyNames;
  private SortedSet<String> clusteringKeyNames;
  private SortedMap<String, String> columns;

  public TableMetadata() {}

  public void setId(String id) {
    this.id = id;
  }

  public void setPartitionKeyNames(Set<String> partitionKeyNames) {
    this.partitionKeyNames = new TreeSet(partitionKeyNames);
  }

  public void setClusteringKeyNames(Set<String> clusteringKeyNames) {
    this.clusteringKeyNames = new TreeSet(clusteringKeyNames);
  }

  public void setColumns(Map<String, String> columns) {
    this.columns = new TreeMap(columns);
  }

  public String getId() {
    return id;
  }

  public Set<String> getPartitionKeyNames() {
    return Collections.unmodifiableSortedSet(partitionKeyNames);
  }

  public Set<String> getClusteringKeyNames() {
    return Collections.unmodifiableSortedSet(clusteringKeyNames);
  }

  public Map<String, String> getColumns() {
    return Collections.unmodifiableSortedMap(columns);
  }
}
