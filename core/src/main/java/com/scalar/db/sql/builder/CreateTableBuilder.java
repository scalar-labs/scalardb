package com.scalar.db.sql.builder;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.scalar.db.sql.DataType;
import com.scalar.db.sql.Order;
import com.scalar.db.sql.statement.CreateTableStatement;

public class CreateTableBuilder {

  private CreateTableBuilder() {}

  public static class Start {
    private final String namespaceName;
    private final String tableName;
    private boolean ifNotExists;

    Start(String namespaceName, String tableName) {
      this.namespaceName = namespaceName;
      this.tableName = tableName;
    }

    public End withPartitionKey(String columnName, DataType dataType) {
      ImmutableMap.Builder<String, DataType> columnsBuilder = ImmutableMap.builder();
      ImmutableSet.Builder<String> partitionKeyColumnNamesBuilder = ImmutableSet.builder();
      columnsBuilder.put(columnName, dataType);
      partitionKeyColumnNamesBuilder.add(columnName);
      return new End(
          namespaceName, tableName, ifNotExists, columnsBuilder, partitionKeyColumnNamesBuilder);
    }

    public Start ifNotExists() {
      ifNotExists = true;
      return this;
    }
  }

  public static class End {
    private final String namespaceName;
    private final String tableName;
    private final boolean ifNotExists;
    private final ImmutableMap.Builder<String, DataType> columnsBuilder;
    private final ImmutableSet.Builder<String> partitionKeyColumnNamesBuilder;
    private final ImmutableSet.Builder<String> clusteringKeyColumnNamesBuilder;
    private final ImmutableMap.Builder<String, Order> clusteringOrdersBuilder;
    private final ImmutableSet.Builder<String> secondaryIndexColumnNamesBuilder;
    private final ImmutableMap.Builder<String, String> optionsBuilder;

    private End(
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
      clusteringKeyColumnNamesBuilder = ImmutableSet.builder();
      clusteringOrdersBuilder = ImmutableMap.builder();
      secondaryIndexColumnNamesBuilder = ImmutableSet.builder();
      optionsBuilder = ImmutableMap.builder();
    }

    public End withPartitionKey(String columnName, DataType dataType) {
      columnsBuilder.put(columnName, dataType);
      partitionKeyColumnNamesBuilder.add(columnName);
      return this;
    }

    public End withClusteringKey(String columnName, DataType dataType) {
      columnsBuilder.put(columnName, dataType);
      clusteringKeyColumnNamesBuilder.add(columnName);
      return this;
    }

    public End withColumn(String columnName, DataType dataType) {
      columnsBuilder.put(columnName, dataType);
      return this;
    }

    public End withClusteringOrder(String columnName, Order clusteringOrder) {
      if (!clusteringKeyColumnNamesBuilder.build().contains(columnName)) {
        throw new IllegalArgumentException(columnName + " is not a clustering key column");
      }

      clusteringOrdersBuilder.put(columnName, clusteringOrder);
      return this;
    }

    public End withSecondaryIndex(String columnName) {
      secondaryIndexColumnNamesBuilder.add(columnName);
      return this;
    }

    public End withOption(String name, String value) {
      optionsBuilder.put(name, value);
      return this;
    }

    public CreateTableStatement build() {
      return new CreateTableStatement(
          namespaceName,
          tableName,
          ifNotExists,
          columnsBuilder.build(),
          partitionKeyColumnNamesBuilder.build(),
          clusteringKeyColumnNamesBuilder.build(),
          clusteringOrdersBuilder.build(),
          secondaryIndexColumnNamesBuilder.build(),
          optionsBuilder.build());
    }
  }
}
