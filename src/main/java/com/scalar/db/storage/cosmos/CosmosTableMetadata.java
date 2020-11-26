package com.scalar.db.storage.cosmos;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.ImmutableSortedSet;
import com.scalar.db.storage.TableMetadata;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;

/**
 * A metadata class for a table of Scalar DB to know the type of each column
 *
 * @author Yuji Ito
 */
public class CosmosTableMetadata implements TableMetadata {
  private String id;
  private SortedSet<String> partitionKeyNames;
  private SortedSet<String> clusteringKeyNames;
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

  public void setColumns(Map<String, String> columns) {
    this.columns = ImmutableSortedMap.copyOf(columns);
  }

  public void setKeyNames(List<String> keyNames) {
    this.keyNames = ImmutableList.copyOf(keyNames);
  }

  public String getId() {
    return id;
  }

  @Override
  public Set<String> getPartitionKeyNames() {
    return ImmutableSortedSet.copyOf(partitionKeyNames);
  }

  @Override
  public Set<String> getClusteringKeyNames() {
    return ImmutableSortedSet.copyOf(clusteringKeyNames);
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
