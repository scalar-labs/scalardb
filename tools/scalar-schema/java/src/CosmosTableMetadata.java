package com.scalar.db.storage.cosmos;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.ImmutableSortedSet;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;

/* Based on Scalar DB
 * https://github.com/scalar-labs/scalardb/blob/master/src/main/java/com/scalar/db/storage/cosmos/CosmosTableMetadata.java
 */
public class CosmosTableMetadata {
  private String id;
  private SortedSet<String> partitionKeyNames;
  private SortedSet<String> clusteringKeyNames;
  private SortedSet<String> secondaryIndexNames;
  private SortedMap<String, String> columns;
  private List<String> keyNames;

  public CosmosTableMetadata() {}

  public void setId(String id) {
    this.id = id;
  }

  public void setPartitionKeyNames(Set<String> partitionKeyNames) {
    this.partitionKeyNames = ImmutableSortedSet.copyOf(partitionKeyNames);
  }

  public void setClusteringKeyNames(Set<String> clusteringKeyNames) {
    this.clusteringKeyNames = ImmutableSortedSet.copyOf(clusteringKeyNames);
  }

  public void setSecondaryIndexNames(Set<String> secondaryIndexNames) {
    this.secondaryIndexNames = ImmutableSortedSet.copyOf(secondaryIndexNames);
  }

  public void setColumns(Map<String, String> columns) {
    this.columns = ImmutableSortedMap.copyOf(columns);
  }

  public void setKeyNames(List<String> keyNames) {
    this.keyNames = ImmutableList.copyOf(keyNames);
  }

  public String getId() {
    return id;
  }

  public Set<String> getPartitionKeyNames() {
    return ImmutableSortedSet.copyOf(partitionKeyNames);
  }

  public Set<String> getClusteringKeyNames() {
    return ImmutableSortedSet.copyOf(clusteringKeyNames);
  }

  public Set<String> getSecondaryIndexNames() {
    return ImmutableSortedSet.copyOf(secondaryIndexNames);
  }

  public Map<String, String> getColumns() {
    return Collections.unmodifiableSortedMap(columns);
  }

  public List<String> getKeyNames() {
    if (keyNames != null) {
      return keyNames;
    }

    keyNames =
        new ImmutableList.Builder<String>()
            .addAll(partitionKeyNames)
            .addAll(clusteringKeyNames)
            .build();

    return keyNames;
  }
}
