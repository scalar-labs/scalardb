package com.scalar.db.sql.statement;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.scalar.db.sql.ClusteringOrder;
import com.scalar.db.sql.DataType;

public class CreateTableStatement implements DdlStatement {

  public final String namespaceName;
  public final String tableName;
  public final boolean ifNotExists;
  public final ImmutableMap<String, DataType> columns;
  public final ImmutableSet<String> partitionKeyColumnNames;
  public final ImmutableSet<String> clusteringKeyColumnNames;
  public final ImmutableMap<String, ClusteringOrder> clusteringOrders;
  public final ImmutableSet<String> secondaryIndexColumnNames;
  public final ImmutableMap<String, String> options;

  public CreateTableStatement(
      String namespaceName,
      String tableName,
      boolean ifNotExists,
      ImmutableMap<String, DataType> columns,
      ImmutableSet<String> partitionKeyColumnNames,
      ImmutableSet<String> clusteringKeyColumnNames,
      ImmutableMap<String, ClusteringOrder> clusteringOrders,
      ImmutableSet<String> secondaryIndexColumnNames,
      ImmutableMap<String, String> options) {
    this.namespaceName = namespaceName;
    this.tableName = tableName;
    this.ifNotExists = ifNotExists;
    this.columns = columns;
    this.partitionKeyColumnNames = partitionKeyColumnNames;
    this.clusteringKeyColumnNames = clusteringKeyColumnNames;
    this.clusteringOrders = clusteringOrders;
    this.secondaryIndexColumnNames = secondaryIndexColumnNames;
    this.options = options;
  }

  @Override
  public void accept(StatementVisitor visitor) {
    visitor.visit(this);
  }

  @Override
  public void accept(DdlStatementVisitor visitor) {
    visitor.visit(this);
  }
}
