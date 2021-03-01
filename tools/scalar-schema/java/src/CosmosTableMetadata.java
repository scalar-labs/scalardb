package com.scalar.db.storage.cosmos;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/* Based on Scalar DB
 * https://github.com/scalar-labs/scalardb/blob/master/src/main/java/com/scalar/db/storage/cosmos/CosmosTableMetadata.java
 */
public class CosmosTableMetadata {
  private String id;
  private LinkedHashSet<String> partitionKeyNames;
  private LinkedHashSet<String> clusteringKeyNames;
  private Set<String> secondaryIndexNames;
  private Map<String, String> columns;
  private List<String> keyNames;

  public CosmosTableMetadata() {}

  public void setId(String id) {
    this.id = id;
  }

  public void setPartitionKeyNames(List<String> partitionKeyNames) {
    this.partitionKeyNames = new LinkedHashSet<>(partitionKeyNames);
  }

  public void setClusteringKeyNames(List<String> clusteringKeyNames) {
    this.clusteringKeyNames = new LinkedHashSet<>(clusteringKeyNames);
  }

  public void setSecondaryIndexNames(Set<String> secondaryIndexNames) {
    this.secondaryIndexNames = ImmutableSortedSet.copyOf(secondaryIndexNames);
  }

  public void setColumns(Map<String, String> columns) {
    this.columns = ImmutableMap.copyOf(columns);
  }

  public void setKeyNames(List<String> keyNames) {
    this.keyNames = ImmutableList.copyOf(keyNames);
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
