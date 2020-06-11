package com.scalar.db.storage.cosmos;

import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.ImmutableSortedSet;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;

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
    this.partitionKeyNames = ImmutableSortedSet.copyOf(partitionKeyNames);
  }

  public void setClusteringKeyNames(Set<String> clusteringKeyNames) {
    this.clusteringKeyNames = ImmutableSortedSet.copyOf(clusteringKeyNames);
  }

  public void setColumns(Map<String, String> columns) {
    this.columns = ImmutableSortedMap.copyOf(columns);
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
