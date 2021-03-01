package com.scalar.db.storage.jdbc.metadata;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.scalar.db.api.Scan;
import com.scalar.db.storage.TableMetadata;

import javax.annotation.concurrent.Immutable;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

@Immutable
public class JdbcTableMetadata implements TableMetadata {

  private final String fullTableName;
  private final String schema;
  private final String table;
  private final ImmutableMap<String, DataType> dataTypes;
  private final ImmutableList<String> partitionKeys;
  private final ImmutableList<String> clusteringKeys;
  private final ImmutableList<String> columns;
  private final ImmutableMap<String, Scan.Ordering.Order> clusteringKeyOrders;
  private final ImmutableSet<String> indexedColumns;
  private final ImmutableMap<String, Scan.Ordering.Order> indexOrders;

  public JdbcTableMetadata(
      String fullTableName,
      LinkedHashMap<String, DataType> columnsAndDataTypes,
      List<String> partitionKeys,
      List<String> clusteringKeys,
      Map<String, Scan.Ordering.Order> clusteringKeyOrders,
      Set<String> indexedColumns,
      Map<String, Scan.Ordering.Order> indexOrders) {
    this.fullTableName = Objects.requireNonNull(fullTableName);
    String[] schemaAndTable = fullTableName.split("\\.");
    schema = schemaAndTable[0];
    table = schemaAndTable[1];
    this.dataTypes = ImmutableMap.copyOf(Objects.requireNonNull(columnsAndDataTypes));
    this.partitionKeys = ImmutableList.copyOf(Objects.requireNonNull(partitionKeys));
    this.clusteringKeys = ImmutableList.copyOf(Objects.requireNonNull(clusteringKeys));
    columns = ImmutableList.copyOf(columnsAndDataTypes.keySet());
    this.clusteringKeyOrders = ImmutableMap.copyOf(Objects.requireNonNull(clusteringKeyOrders));
    this.indexedColumns = ImmutableSet.copyOf(indexedColumns);
    this.indexOrders = ImmutableMap.copyOf(indexOrders);
  }

  public JdbcTableMetadata(
      String schema,
      String table,
      LinkedHashMap<String, DataType> columnsAndDataTypes,
      List<String> partitionKeys,
      List<String> clusteringKeys,
      Map<String, Scan.Ordering.Order> clusteringKeyOrders,
      Set<String> indexedColumns,
      Map<String, Scan.Ordering.Order> indexOrders) {
    this.schema = Objects.requireNonNull(schema);
    this.table = Objects.requireNonNull(table);
    this.fullTableName = schema + "." + table;
    this.dataTypes = ImmutableMap.copyOf(Objects.requireNonNull(columnsAndDataTypes));
    this.partitionKeys = ImmutableList.copyOf(Objects.requireNonNull(partitionKeys));
    this.clusteringKeys = ImmutableList.copyOf(Objects.requireNonNull(clusteringKeys));
    columns = ImmutableList.copyOf(columnsAndDataTypes.keySet());
    this.clusteringKeyOrders = ImmutableMap.copyOf(Objects.requireNonNull(clusteringKeyOrders));
    this.indexedColumns = ImmutableSet.copyOf(indexedColumns);
    this.indexOrders = ImmutableMap.copyOf(indexOrders);
  }

  public String getFullTableName() {
    return fullTableName;
  }

  public String getSchema() {
    return schema;
  }

  public String getTable() {
    return table;
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
  public boolean isIndexedColumn(String column) {
    return indexedColumns.contains(column);
  }

  public Scan.Ordering.Order getIndexOrder(String column) {
    return indexOrders.get(column);
  }

  @Override
  public LinkedHashSet<String> getPartitionKeyNames() {
    return new LinkedHashSet<>(partitionKeys);
  }

  @Override
  public LinkedHashSet<String> getClusteringKeyNames() {
    return new LinkedHashSet<>(partitionKeys);
  }

  @Override
  public Set<String> getSecondaryIndexNames() {
    return indexedColumns;
  }
}
