package com.scalar.db.api;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.scalar.db.api.Scan.Ordering.Order;
import com.scalar.db.common.error.CoreError;
import com.scalar.db.io.DataType;
import com.scalar.db.util.ImmutableLinkedHashSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.annotation.concurrent.Immutable;

/** A class that represents the table metadata. */
@Immutable
public class TableMetadata {

  private final ImmutableLinkedHashSet<String> columnNames;
  private final ImmutableMap<String, DataType> columnDataTypes;
  private final ImmutableLinkedHashSet<String> partitionKeyNames;
  private final ImmutableLinkedHashSet<String> clusteringKeyNames;
  private final ImmutableMap<String, Order> clusteringOrders;
  private final ImmutableSet<String> secondaryIndexNames;
  private final ImmutableSet<String> encryptedColumnNames;

  private TableMetadata(
      LinkedHashMap<String, DataType> columns,
      LinkedHashSet<String> partitionKeyNames,
      LinkedHashSet<String> clusteringKeyNames,
      Map<String, Order> clusteringOrders,
      Set<String> secondaryIndexNames,
      Set<String> encryptedColumnNames) {
    columnNames = new ImmutableLinkedHashSet<>(Objects.requireNonNull(columns.keySet()));
    columnDataTypes = ImmutableMap.copyOf(Objects.requireNonNull(columns));
    this.partitionKeyNames =
        new ImmutableLinkedHashSet<>(Objects.requireNonNull(partitionKeyNames));
    this.clusteringKeyNames =
        new ImmutableLinkedHashSet<>(Objects.requireNonNull(clusteringKeyNames));
    this.clusteringOrders = ImmutableMap.copyOf(Objects.requireNonNull(clusteringOrders));
    this.secondaryIndexNames = ImmutableSet.copyOf(Objects.requireNonNull(secondaryIndexNames));
    this.encryptedColumnNames = ImmutableSet.copyOf(Objects.requireNonNull(encryptedColumnNames));
  }

  /**
   * Creates a new builder instance.
   *
   * @return a new builder instance
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Creates a new builder instance based on a prototype.
   *
   * @param prototype a prototype for a new builder
   * @return a new builder instance
   */
  public static Builder newBuilder(TableMetadata prototype) {
    return new Builder(prototype);
  }

  /**
   * Returns the column names of the table.
   *
   * @return an {@code LinkedHashSet} of the column names
   */
  public LinkedHashSet<String> getColumnNames() {
    return columnNames;
  }

  /**
   * Returns the data type of the specified column.
   *
   * @param columnName a column name to retrieve the data type
   * @return the {@code DataType} of the specified column
   */
  public DataType getColumnDataType(String columnName) {
    return columnDataTypes.get(columnName);
  }

  /**
   * Returns the map of the columns names and the data types.
   *
   * @return the map of the columns names and the data types
   */
  public Map<String, DataType> getColumnDataTypes() {
    return columnDataTypes;
  }

  /**
   * Returns the partition-key column names.
   *
   * @return an {@code LinkedHashSet} of the partition-key column names
   */
  public LinkedHashSet<String> getPartitionKeyNames() {
    return partitionKeyNames;
  }

  /**
   * Returns the clustering-key column names.
   *
   * @return an {@code LinkedHashSet} of the clustering-key column names
   */
  public LinkedHashSet<String> getClusteringKeyNames() {
    return clusteringKeyNames;
  }

  /**
   * Returns the clustering order of the specified clustering-key column.
   *
   * @param clusteringKeyName a clustering-key column name to retrieve the order
   * @return the clustering order of the specified clustering-key column
   */
  public Scan.Ordering.Order getClusteringOrder(String clusteringKeyName) {
    return clusteringOrders.get(clusteringKeyName);
  }

  /**
   * Returns the map of the clustering-key column names and the clustering orders.
   *
   * @return the map of the clustering-key column names and the clustering orders
   */
  public Map<String, Scan.Ordering.Order> getClusteringOrders() {
    return clusteringOrders;
  }

  /**
   * Returns the secondary-index column names.
   *
   * @return an {@code Set} of the secondary-index column names
   */
  public Set<String> getSecondaryIndexNames() {
    return secondaryIndexNames;
  }

  /**
   * Returns the encrypted column names.
   *
   * @return an {@code Set} of the encrypted column names
   */
  public Set<String> getEncryptedColumnNames() {
    return encryptedColumnNames;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TableMetadata)) {
      return false;
    }

    TableMetadata metadata = (TableMetadata) o;
    return Objects.equals(columnNames, metadata.columnNames)
        && Objects.equals(columnDataTypes, metadata.columnDataTypes)
        && Objects.equals(partitionKeyNames, metadata.partitionKeyNames)
        && Objects.equals(clusteringKeyNames, metadata.clusteringKeyNames)
        && Objects.equals(clusteringOrders, metadata.clusteringOrders)
        && Objects.equals(secondaryIndexNames, metadata.secondaryIndexNames)
        && Objects.equals(encryptedColumnNames, metadata.encryptedColumnNames);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        columnNames,
        columnDataTypes,
        partitionKeyNames,
        clusteringKeyNames,
        clusteringOrders,
        secondaryIndexNames,
        encryptedColumnNames);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("columnNames", columnNames)
        .add("columnDataTypes", columnDataTypes)
        .add("partitionKeyNames", partitionKeyNames)
        .add("clusteringKeyNames", clusteringKeyNames)
        .add("clusteringOrders", clusteringOrders)
        .add("secondaryIndexNames", secondaryIndexNames)
        .add("encryptedColumnNames", encryptedColumnNames)
        .toString();
  }

  /** A builder class that creates a TableMetadata instance. */
  public static final class Builder {
    private final LinkedHashMap<String, DataType> columns = new LinkedHashMap<>();
    private final LinkedHashSet<String> partitionKeyNames = new LinkedHashSet<>();
    private final LinkedHashSet<String> clusteringKeyNames = new LinkedHashSet<>();
    private final Map<String, Order> clusteringOrders = new HashMap<>();
    private final Set<String> secondaryIndexNames = new HashSet<>();
    private final Set<String> encryptedColumnNames = new HashSet<>();

    private Builder() {}

    private Builder(TableMetadata prototype) {
      columns.putAll(prototype.columnDataTypes);
      partitionKeyNames.addAll(prototype.partitionKeyNames);
      clusteringKeyNames.addAll(prototype.clusteringKeyNames);
      clusteringOrders.putAll(prototype.clusteringOrders);
      secondaryIndexNames.addAll(prototype.secondaryIndexNames);
      encryptedColumnNames.addAll(prototype.encryptedColumnNames);
    }

    /**
     * Adds a column with the specified name and data type.
     *
     * @param name a column name
     * @param type a data type of the column
     * @return a builder instance
     */
    public Builder addColumn(String name, DataType type) {
      return addColumn(name, type, false);
    }

    /**
     * Adds a column with the specified name, data type, and encryption flag.
     *
     * @param name a column name
     * @param type a data type of the column
     * @param encrypted whether the column is encrypted
     * @return a builder instance
     */
    public Builder addColumn(String name, DataType type, boolean encrypted) {
      columns.put(name, type);
      if (encrypted) {
        encryptedColumnNames.add(name);
      }
      return this;
    }

    /**
     * Removes the column with the specified name.
     *
     * @param name a column name
     * @return a builder instance
     */
    public Builder removeColumn(String name) {
      columns.remove(name);
      encryptedColumnNames.remove(name);
      return this;
    }

    /**
     * Adds a partition-key column with the specified name.
     *
     * @param name a column name
     * @return a builder instance
     */
    public Builder addPartitionKey(String name) {
      partitionKeyNames.add(name);
      return this;
    }

    /**
     * Removes the partition-key column with the specified name.
     *
     * @param name a column name
     * @return a builder instance
     */
    public Builder removePartitionKey(String name) {
      partitionKeyNames.remove(name);
      return this;
    }

    /**
     * Adds a clustering-key column with the specified name and the default order (ASC).
     *
     * @param name a column name
     * @return a builder instance
     */
    public Builder addClusteringKey(String name) {
      addClusteringKey(name, Scan.Ordering.Order.ASC);
      return this;
    }

    /**
     * Adds a clustering-key column with the specified name and the specified order.
     *
     * @param name a column name
     * @param clusteringOrder a clustering order
     * @return a builder instance
     */
    public Builder addClusteringKey(String name, Scan.Ordering.Order clusteringOrder) {
      clusteringKeyNames.add(name);
      clusteringOrders.put(name, clusteringOrder);
      return this;
    }

    /**
     * Removes the clustering-key column with the specified name.
     *
     * @param name a column name
     * @return a builder instance
     */
    public Builder removeClusteringKey(String name) {
      clusteringKeyNames.remove(name);
      clusteringOrders.remove(name);
      return this;
    }

    /**
     * Adds a secondary-index column with the specified name.
     *
     * @param name a column name
     * @return a builder instance
     */
    public Builder addSecondaryIndex(String name) {
      secondaryIndexNames.add(name);
      return this;
    }

    /**
     * Removes the secondary-index column with the specified name.
     *
     * @param name a column name
     * @return a builder instance
     */
    public Builder removeSecondaryIndex(String name) {
      secondaryIndexNames.remove(name);
      return this;
    }

    /**
     * Builds a TableMetadata instance.
     *
     * @throws IllegalStateException if no columns are specified, no partition-key columns are
     *     specified, a partition-key column is not specified in the column definitions, or a
     *     clustering-key column is not specified in the column definitions
     * @return a TableMetadata instance
     */
    public TableMetadata build() {
      if (columns.isEmpty()) {
        throw new IllegalStateException(
            CoreError.TABLE_METADATA_BUILD_ERROR_NO_COLUMNS_SPECIFIED.buildMessage());
      }
      if (partitionKeyNames.isEmpty()) {
        throw new IllegalStateException(
            CoreError.TABLE_METADATA_BUILD_ERROR_NO_PARTITION_KEYS_SPECIFIED.buildMessage());
      }
      partitionKeyNames.forEach(
          k -> {
            if (!columns.containsKey(k)) {
              throw new IllegalStateException(
                  CoreError.TABLE_METADATA_BUILD_ERROR_PARTITION_KEY_COLUMN_DEFINITION_NOT_SPECIFIED
                      .buildMessage(k));
            }
          });
      clusteringKeyNames.forEach(
          k -> {
            if (!columns.containsKey(k)) {
              throw new IllegalStateException(
                  CoreError
                      .TABLE_METADATA_BUILD_ERROR_CLUSTERING_KEY_COLUMN_DEFINITION_NOT_SPECIFIED
                      .buildMessage(k));
            }
          });
      return new TableMetadata(
          columns,
          partitionKeyNames,
          clusteringKeyNames,
          clusteringOrders,
          secondaryIndexNames,
          encryptedColumnNames);
    }
  }
}
