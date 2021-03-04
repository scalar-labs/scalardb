package com.scalar.db.storage.jdbc.metadata;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.scalar.db.api.Scan;
import com.scalar.db.storage.common.metadata.DataType;
import com.scalar.db.storage.common.metadata.TableMetadata;
import com.scalar.db.storage.common.util.ImmutableLinkedHashSet;

import javax.annotation.concurrent.Immutable;
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
  private final LinkedHashSet<String> partitionKeyNames;
  private final LinkedHashSet<String> clusteringKeyNames;
  private final ImmutableMap<String, Scan.Ordering.Order> clusteringOrders;
  private final Map<String, DataType> columnDataTypes;
  private final Set<String> secondaryIndexNames;
  private final ImmutableMap<String, Scan.Ordering.Order> secondaryIndexOrders;

  public JdbcTableMetadata(
      String fullTableName,
      List<String> partitionKeyNames,
      List<String> clusteringKeyNames,
      Map<String, Scan.Ordering.Order> clusteringOrders,
      Map<String, DataType> columnDataTypes,
      List<String> secondaryIndexNames,
      Map<String, Scan.Ordering.Order> secondaryIndexOrders) {
    this.fullTableName = Objects.requireNonNull(fullTableName);
    String[] schemaAndTable = fullTableName.split("\\.");
    schema = schemaAndTable[0];
    table = schemaAndTable[1];
    this.partitionKeyNames =
        new ImmutableLinkedHashSet<>(Objects.requireNonNull(partitionKeyNames));
    this.clusteringKeyNames =
        new ImmutableLinkedHashSet<>(Objects.requireNonNull(clusteringKeyNames));
    this.clusteringOrders = ImmutableMap.copyOf(Objects.requireNonNull(clusteringOrders));
    this.columnDataTypes = ImmutableMap.copyOf(Objects.requireNonNull(columnDataTypes));
    this.secondaryIndexNames = ImmutableSet.copyOf(Objects.requireNonNull(secondaryIndexNames));
    this.secondaryIndexOrders = ImmutableMap.copyOf(Objects.requireNonNull(secondaryIndexOrders));
  }

  public JdbcTableMetadata(
      String schema,
      String table,
      List<String> partitionKeyNames,
      List<String> clusteringKeyNames,
      Map<String, Scan.Ordering.Order> clusteringOrders,
      Map<String, DataType> columnDataTypes,
      List<String> secondaryIndexNames,
      Map<String, Scan.Ordering.Order> secondaryIndexOrders) {
    this.schema = Objects.requireNonNull(schema);
    this.table = Objects.requireNonNull(table);
    this.fullTableName = schema + "." + table;
    this.partitionKeyNames =
        new ImmutableLinkedHashSet<>(Objects.requireNonNull(partitionKeyNames));
    this.clusteringKeyNames =
        new ImmutableLinkedHashSet<>(Objects.requireNonNull(clusteringKeyNames));
    this.clusteringOrders = ImmutableMap.copyOf(Objects.requireNonNull(clusteringOrders));
    this.columnDataTypes = ImmutableMap.copyOf(Objects.requireNonNull(columnDataTypes));
    this.secondaryIndexNames = ImmutableSet.copyOf(Objects.requireNonNull(secondaryIndexNames));
    this.secondaryIndexOrders = ImmutableMap.copyOf(Objects.requireNonNull(secondaryIndexOrders));
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

  @Override
  public LinkedHashSet<String> getPartitionKeyNames() {
    return partitionKeyNames;
  }

  @Override
  public LinkedHashSet<String> getClusteringKeyNames() {
    return clusteringKeyNames;
  }

  @Override
  public Scan.Ordering.Order getClusteringOrder(String clusteringKeyName) {
    return clusteringOrders.get(clusteringKeyName);
  }

  @Override
  public Set<String> getColumnNames() {
    return columnDataTypes.keySet();
  }

  @Override
  public DataType getColumnDataType(String columnName) {
    return columnDataTypes.get(columnName);
  }

  @Override
  public Set<String> getSecondaryIndexNames() {
    return secondaryIndexNames;
  }

  public Scan.Ordering.Order getSecondaryIndexOrder(String column) {
    return secondaryIndexOrders.get(column);
  }
}
