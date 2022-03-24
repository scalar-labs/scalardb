package com.scalar.db.sql.statement;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.scalar.db.sql.ClusteringOrder;
import com.scalar.db.sql.DataType;
import java.util.Objects;
import javax.annotation.concurrent.Immutable;

@Immutable
public class CreateTableStatement implements DdlStatement {

  public final String namespaceName;
  public final String tableName;
  public final boolean ifNotExists;
  public final ImmutableMap<String, DataType> columns;
  public final ImmutableSet<String> partitionKeyColumnNames;
  public final ImmutableSet<String> clusteringKeyColumnNames;
  public final ImmutableMap<String, ClusteringOrder> clusteringOrders;
  public final ImmutableSet<String> indexColumnNames;
  public final ImmutableMap<String, String> options;

  public CreateTableStatement(
      String namespaceName,
      String tableName,
      boolean ifNotExists,
      ImmutableMap<String, DataType> columns,
      ImmutableSet<String> partitionKeyColumnNames,
      ImmutableSet<String> clusteringKeyColumnNames,
      ImmutableMap<String, ClusteringOrder> clusteringOrders,
      ImmutableSet<String> indexColumnNames,
      ImmutableMap<String, String> options) {
    this.namespaceName = Objects.requireNonNull(namespaceName);
    this.tableName = Objects.requireNonNull(tableName);
    this.ifNotExists = ifNotExists;
    this.columns = Objects.requireNonNull(columns);
    this.partitionKeyColumnNames = Objects.requireNonNull(partitionKeyColumnNames);
    this.clusteringKeyColumnNames = Objects.requireNonNull(clusteringKeyColumnNames);
    this.clusteringOrders = Objects.requireNonNull(clusteringOrders);
    this.indexColumnNames = Objects.requireNonNull(indexColumnNames);
    this.options = Objects.requireNonNull(options);
  }

  @Override
  public void accept(StatementVisitor visitor) {
    visitor.visit(this);
  }

  @Override
  public void accept(DdlStatementVisitor visitor) {
    visitor.visit(this);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("namespaceName", namespaceName)
        .add("tableName", tableName)
        .add("ifNotExists", ifNotExists)
        .add("columns", columns)
        .add("partitionKeyColumnNames", partitionKeyColumnNames)
        .add("clusteringKeyColumnNames", clusteringKeyColumnNames)
        .add("clusteringOrders", clusteringOrders)
        .add("indexColumnNames", indexColumnNames)
        .add("options", options)
        .toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof CreateTableStatement)) {
      return false;
    }
    CreateTableStatement that = (CreateTableStatement) o;
    return ifNotExists == that.ifNotExists
        && Objects.equals(namespaceName, that.namespaceName)
        && Objects.equals(tableName, that.tableName)
        && Objects.equals(columns, that.columns)
        && Objects.equals(partitionKeyColumnNames, that.partitionKeyColumnNames)
        && Objects.equals(clusteringKeyColumnNames, that.clusteringKeyColumnNames)
        && Objects.equals(clusteringOrders, that.clusteringOrders)
        && Objects.equals(indexColumnNames, that.indexColumnNames)
        && Objects.equals(options, that.options);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        namespaceName,
        tableName,
        ifNotExists,
        columns,
        partitionKeyColumnNames,
        clusteringKeyColumnNames,
        clusteringOrders,
        indexColumnNames,
        options);
  }
}
