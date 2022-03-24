package com.scalar.db.sql.builder;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.scalar.db.sql.ClusteringOrder;
import com.scalar.db.sql.DataType;
import com.scalar.db.sql.statement.CreateTableStatement;
import java.util.Map;
import java.util.Set;

public class CreateTableStatementBuilder {

  private CreateTableStatementBuilder() {}

  public static class Start extends WithPartitionKey {
    Start(String namespaceName, String tableName) {
      super(namespaceName, tableName, false);
    }

    public WithPartitionKey ifNotExists() {
      return new WithPartitionKey(namespaceName, tableName, true);
    }
  }

  public static class WithPartitionKey {
    protected final String namespaceName;
    protected final String tableName;
    private final boolean ifNotExists;

    private WithPartitionKey(String namespaceName, String tableName, boolean ifNotExists) {
      this.namespaceName = namespaceName;
      this.tableName = tableName;
      this.ifNotExists = ifNotExists;
    }

    public Buildable withPartitionKey(String columnName, DataType dataType) {
      ImmutableMap.Builder<String, DataType> columnsBuilder = ImmutableMap.builder();
      columnsBuilder.put(columnName, dataType);
      ImmutableSet.Builder<String> partitionKeyColumnNamesBuilder = ImmutableSet.builder();
      partitionKeyColumnNamesBuilder.add(columnName);
      return new Buildable(
          namespaceName, tableName, ifNotExists, columnsBuilder, partitionKeyColumnNamesBuilder);
    }

    public Buildable withPartitionKey(Map<String, DataType> columnNameAndDataTypeMap) {
      ImmutableMap.Builder<String, DataType> columnsBuilder = ImmutableMap.builder();
      columnsBuilder.putAll(columnNameAndDataTypeMap);
      ImmutableSet.Builder<String> partitionKeyColumnNamesBuilder = ImmutableSet.builder();
      partitionKeyColumnNamesBuilder.addAll(columnNameAndDataTypeMap.keySet());
      return new Buildable(
          namespaceName, tableName, ifNotExists, columnsBuilder, partitionKeyColumnNamesBuilder);
    }
  }

  public static class Buildable {
    private final String namespaceName;
    private final String tableName;
    private final boolean ifNotExists;
    private final ImmutableMap.Builder<String, DataType> columnsBuilder;
    private final ImmutableSet.Builder<String> partitionKeyColumnNamesBuilder;
    private final ImmutableSet.Builder<String> clusteringKeyColumnNamesBuilder =
        ImmutableSet.builder();
    private final ImmutableMap.Builder<String, ClusteringOrder> clusteringOrdersBuilder =
        ImmutableMap.builder();
    private final ImmutableSet.Builder<String> indexColumnNamesBuilder = ImmutableSet.builder();
    private final ImmutableMap.Builder<String, String> optionsBuilder = ImmutableMap.builder();

    private Buildable(
        String namespaceName,
        String tableName,
        boolean ifNotExists,
        ImmutableMap.Builder<String, DataType> columnsBuilder,
        ImmutableSet.Builder<String> partitionKeyColumnNamesBuilder) {
      this.namespaceName = namespaceName;
      this.tableName = tableName;
      this.ifNotExists = ifNotExists;
      this.columnsBuilder = columnsBuilder;
      this.partitionKeyColumnNamesBuilder = partitionKeyColumnNamesBuilder;
    }

    public Buildable withPartitionKey(String columnName, DataType dataType) {
      columnsBuilder.put(columnName, dataType);
      partitionKeyColumnNamesBuilder.add(columnName);
      return this;
    }

    public Buildable withPartitionKey(Map<String, DataType> columnNameAndDataTypeMap) {
      columnsBuilder.putAll(columnNameAndDataTypeMap);
      partitionKeyColumnNamesBuilder.addAll(columnNameAndDataTypeMap.keySet());
      return this;
    }

    public Buildable withClusteringKey(String columnName, DataType dataType) {
      columnsBuilder.put(columnName, dataType);
      clusteringKeyColumnNamesBuilder.add(columnName);
      return this;
    }

    public Buildable withClusteringKey(Map<String, DataType> columnNameAndDataTypeMap) {
      columnsBuilder.putAll(columnNameAndDataTypeMap);
      clusteringKeyColumnNamesBuilder.addAll(columnNameAndDataTypeMap.keySet());
      return this;
    }

    public Buildable withColumn(String columnName, DataType dataType) {
      columnsBuilder.put(columnName, dataType);
      return this;
    }

    public Buildable withColumns(Map<String, DataType> columnNameAndDataTypeMap) {
      columnsBuilder.putAll(columnNameAndDataTypeMap);
      return this;
    }

    public Buildable withClusteringOrder(String columnName, ClusteringOrder clusteringOrder) {
      if (!clusteringKeyColumnNamesBuilder.build().contains(columnName)) {
        throw new IllegalArgumentException(columnName + " is not a clustering key column");
      }

      clusteringOrdersBuilder.put(columnName, clusteringOrder);
      return this;
    }

    public Buildable withClusteringOrders(
        Map<String, ClusteringOrder> columnNameAndClusteringOrderMap) {
      columnNameAndClusteringOrderMap
          .keySet()
          .forEach(
              c -> {
                if (!clusteringKeyColumnNamesBuilder.build().contains(c)) {
                  throw new IllegalArgumentException(c + " is not a clustering key column");
                }
              });
      clusteringOrdersBuilder.putAll(columnNameAndClusteringOrderMap);
      return this;
    }

    public Buildable withIndex(String columnName) {
      indexColumnNamesBuilder.add(columnName);
      return this;
    }

    public Buildable withIndexes(Set<String> columnNames) {
      indexColumnNamesBuilder.addAll(columnNames);
      return this;
    }

    public Buildable withOption(String name, String value) {
      optionsBuilder.put(name, value);
      return this;
    }

    public Buildable withOptions(Map<String, String> options) {
      optionsBuilder.putAll(options);
      return this;
    }

    public CreateTableStatement build() {
      return CreateTableStatement.of(
          namespaceName,
          tableName,
          ifNotExists,
          columnsBuilder.build(),
          partitionKeyColumnNamesBuilder.build(),
          clusteringKeyColumnNamesBuilder.build(),
          clusteringOrdersBuilder.build(),
          indexColumnNamesBuilder.build(),
          optionsBuilder.build());
    }
  }
}
