package com.scalar.db.sql;

import com.scalar.db.api.DistributedTransactionAdmin;
import com.scalar.db.api.Scan;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.sql.exception.SqlException;
import com.scalar.db.sql.statement.CreateCoordinatorTableStatement;
import com.scalar.db.sql.statement.CreateIndexStatement;
import com.scalar.db.sql.statement.CreateNamespaceStatement;
import com.scalar.db.sql.statement.CreateTableStatement;
import com.scalar.db.sql.statement.DdlStatement;
import com.scalar.db.sql.statement.DdlStatementVisitor;
import com.scalar.db.sql.statement.DropCoordinatorTableStatement;
import com.scalar.db.sql.statement.DropIndexStatement;
import com.scalar.db.sql.statement.DropNamespaceStatement;
import com.scalar.db.sql.statement.DropTableStatement;
import com.scalar.db.sql.statement.TruncateCoordinatorTableStatement;
import com.scalar.db.sql.statement.TruncateTableStatement;

public class DdlStatementExecutor implements DdlStatementVisitor {

  private final DistributedTransactionAdmin admin;
  private final DdlStatement statement;

  public DdlStatementExecutor(DistributedTransactionAdmin admin, DdlStatement statement) {
    this.admin = admin;
    this.statement = statement;
  }

  public void execute() {
    statement.accept(this);
  }

  @Override
  public void visit(CreateNamespaceStatement statement) {
    try {
      admin.createNamespace(statement.namespaceName, statement.ifNotExists, statement.options);
    } catch (ExecutionException e) {
      throw new SqlException("Failed to create a namespace", e);
    }
  }

  @Override
  public void visit(CreateTableStatement statement) {
    try {
      TableMetadata tableMetadata = convertCreateStatementToTableMetadata(statement);
      admin.createTable(
          statement.namespaceName,
          statement.tableName,
          tableMetadata,
          statement.ifNotExists,
          statement.options);
    } catch (ExecutionException e) {
      throw new SqlException("Failed to create a table", e);
    }
  }

  private static com.scalar.db.api.TableMetadata convertCreateStatementToTableMetadata(
      CreateTableStatement statement) {
    com.scalar.db.api.TableMetadata.Builder builder = com.scalar.db.api.TableMetadata.newBuilder();
    statement.columns.forEach((c, d) -> builder.addColumn(c, convertDataType(d)));
    statement.partitionKeyColumnNames.forEach(builder::addPartitionKey);
    statement.clusteringKeyColumnNames.forEach(
        n ->
            builder.addClusteringKey(
                n,
                convertClusteringOrder(
                    statement.clusteringOrders.getOrDefault(n, ClusteringOrder.ASC))));
    statement.indexColumnNames.forEach(builder::addSecondaryIndex);
    return builder.build();
  }

  private static com.scalar.db.io.DataType convertDataType(DataType dataType) {
    switch (dataType) {
      case BOOLEAN:
        return com.scalar.db.io.DataType.BOOLEAN;
      case INT:
        return com.scalar.db.io.DataType.INT;
      case BIGINT:
        return com.scalar.db.io.DataType.BIGINT;
      case FLOAT:
        return com.scalar.db.io.DataType.FLOAT;
      case DOUBLE:
        return com.scalar.db.io.DataType.DOUBLE;
      case TEXT:
        return com.scalar.db.io.DataType.TEXT;
      case BLOB:
        return com.scalar.db.io.DataType.BLOB;
      default:
        throw new AssertionError();
    }
  }

  private static Scan.Ordering.Order convertClusteringOrder(ClusteringOrder clusteringOrder) {
    switch (clusteringOrder) {
      case ASC:
        return Scan.Ordering.Order.ASC;
      case DESC:
        return Scan.Ordering.Order.DESC;
      default:
        throw new AssertionError();
    }
  }

  @Override
  public void visit(DropNamespaceStatement statement) {
    try {
      if (statement.cascade) {
        for (String tableName : admin.getNamespaceTableNames(statement.namespaceName)) {
          admin.dropTable(statement.namespaceName, tableName);
        }
      }
      admin.dropNamespace(statement.namespaceName, statement.ifExists);
    } catch (ExecutionException e) {
      throw new SqlException("Failed to drop a namespace", e);
    }
  }

  @Override
  public void visit(DropTableStatement statement) {
    try {
      admin.dropTable(statement.namespaceName, statement.tableName, statement.ifExists);
    } catch (ExecutionException e) {
      throw new SqlException("Failed to drop a table", e);
    }
  }

  @Override
  public void visit(TruncateTableStatement statement) {
    try {
      admin.truncateTable(statement.namespaceName, statement.tableName);
    } catch (ExecutionException e) {
      throw new SqlException("Failed to drop a table", e);
    }
  }

  @Override
  public void visit(CreateCoordinatorTableStatement statement) {
    try {
      admin.createCoordinatorNamespaceAndTable(statement.ifNotExists, statement.options);
    } catch (ExecutionException e) {
      throw new SqlException("Failed to create a coordinator table", e);
    }
  }

  @Override
  public void visit(DropCoordinatorTableStatement statement) {
    try {
      admin.dropCoordinatorNamespaceAndTable(statement.ifExists);
    } catch (ExecutionException e) {
      throw new SqlException("Failed to drop a coordinator table", e);
    }
  }

  @Override
  public void visit(TruncateCoordinatorTableStatement statement) {
    try {
      admin.truncateCoordinatorTable();
    } catch (ExecutionException e) {
      throw new SqlException("Failed to truncate a coordinator table", e);
    }
  }

  @Override
  public void visit(CreateIndexStatement statement) {
    try {
      admin.createIndex(
          statement.namespaceName,
          statement.tableName,
          statement.columnName,
          statement.ifNotExists,
          statement.options);
    } catch (ExecutionException e) {
      throw new SqlException("Failed to create a index table", e);
    }
  }

  @Override
  public void visit(DropIndexStatement statement) {
    try {
      admin.dropIndex(
          statement.namespaceName, statement.tableName, statement.columnName, statement.ifExists);
    } catch (ExecutionException e) {
      throw new SqlException("Failed to drop a index table", e);
    }
  }
}
