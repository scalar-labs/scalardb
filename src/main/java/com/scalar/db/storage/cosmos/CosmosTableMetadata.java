package com.scalar.db.storage.cosmos;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedSet;
import com.scalar.db.api.Scan;
import com.scalar.db.exception.storage.UnsupportedTypeException;
import com.scalar.db.storage.common.metadata.DataType;
import com.scalar.db.storage.common.metadata.TableMetadata;
import com.scalar.db.storage.common.util.ImmutableLinkedHashSet;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A metadata class for a table of Scalar DB to know the type of each column
 *
 * @author Yuji Ito
 */
public class CosmosTableMetadata implements TableMetadata {
  private String id;
  private LinkedHashSet<String> partitionKeyNames;
  private LinkedHashSet<String> clusteringKeyNames;
  private Set<String> secondaryIndexNames;
  private Map<String, String> columns;
  private Map<String, DataType> columnDataTypes;
  private List<String> keyNames;

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
    columnDataTypes =
        ImmutableMap.copyOf(
            columns.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> convertDataType(e.getValue()))));
  }

  private DataType convertDataType(String columnType) {
    switch (columnType) {
      case "int":
        return DataType.INT;
      case "bigint":
        return DataType.BIGINT;
      case "float":
        return DataType.FLOAT;
      case "double":
        return DataType.DOUBLE;
      case "text": // for backwards compatibility
      case "varchar":
        return DataType.TEXT;
      case "boolean":
        return DataType.BOOLEAN;
      case "blob":
        return DataType.BLOB;
      default:
        throw new UnsupportedTypeException(columnType);
    }
  }

  public void setKeyNames(List<String> keyNames) {
    this.keyNames = ImmutableList.copyOf(keyNames);
  }

  public String getId() {
    return id;
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
  public Set<String> getSecondaryIndexNames() {
    return secondaryIndexNames;
  }

  public Map<String, String> getColumns() {
    return columns;
  }

  @JsonIgnore
  @Override
  public Set<String> getColumnNames() {
    return columnDataTypes.keySet();
  }

  @JsonIgnore
  @Override
  public DataType getColumnDataType(String columnName) {
    return columnDataTypes.get(columnName);
  }

  @JsonIgnore
  @Override
  public Scan.Ordering.Order getClusteringOrder(String clusteringKeyName) {
    // Always returns ASC for now if the clustering key name exists
    return clusteringKeyNames.contains(clusteringKeyName) ? Scan.Ordering.Order.ASC : null;
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
