package com.scalar.db.storage.jdbc.metadata;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.scalar.db.api.Scan;

import javax.annotation.concurrent.Immutable;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

@Immutable
public class JdbcTableMetadata implements com.scalar.db.storage.TableMetadata {

  private final String fullTableName;
  private final ImmutableMap<String, DataType> dataTypes;
  private final ImmutableList<String> partitionKeys;
  private final ImmutableList<String> clusteringKeys;
  private final ImmutableList<String> columns;
  private final ImmutableMap<String, Scan.Ordering.Order> clusteringKeyOrders;
  private final ImmutableSet<String> indexedColumns;

  public JdbcTableMetadata(
      String fullTableName,
      Map<String, DataType> dataTypes,
      List<String> partitionKeys,
      List<String> clusteringKeys,
      Map<String, Scan.Ordering.Order> clusteringKeyOrders,
      Set<String> indexedColumns) {
    this.fullTableName = Objects.requireNonNull(fullTableName);
    this.dataTypes = ImmutableMap.copyOf(Objects.requireNonNull(dataTypes));
    this.partitionKeys = ImmutableList.copyOf(Objects.requireNonNull(partitionKeys));
    this.clusteringKeys = ImmutableList.copyOf(Objects.requireNonNull(clusteringKeys));
    columns = ImmutableList.copyOf(dataTypes.keySet());
    this.clusteringKeyOrders = ImmutableMap.copyOf(Objects.requireNonNull(clusteringKeyOrders));
    this.indexedColumns = ImmutableSet.copyOf(indexedColumns);
  }

  public String getfullTableName() {
    return fullTableName;
  }

  public List<String> getPartitionKeys() {
    return partitionKeys;
  }

  public List<String> getClusteringKeys() {
    return clusteringKeys;
  }

  public List<String> getColumns() {
    return columns;
  }

  public DataType getDataType(String column) {
    return dataTypes.get(column);
  }

  public Scan.Ordering.Order getClusteringKeyOrder(String clusteringKeyName) {
    return clusteringKeyOrders.get(clusteringKeyName);
  }

  public boolean columnExists(String column) {
    return dataTypes.containsKey(column);
  }

  /** @return whether or not the specified column is indexed */
  public boolean indexedColumn(String column) {
    return indexedColumns.contains(column);
  }

  @Override
  public Set<String> getPartitionKeyNames() {
    return new LinkedHashSet<>(partitionKeys);
  }

  @Override
  public Set<String> getClusteringKeyNames() {
    return new LinkedHashSet<>(partitionKeys);
  }
}
