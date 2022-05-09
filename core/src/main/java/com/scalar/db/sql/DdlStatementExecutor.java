package com.scalar.db.sql;

import com.scalar.db.api.DistributedTransactionAdmin;
import com.scalar.db.api.Scan;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.sql.exception.SqlException;
import com.scalar.db.sql.metadata.CachedMetadata;
import com.scalar.db.sql.metadata.CachedNamespaceMetadata;
import com.scalar.db.sql.metadata.Metadata;
import com.scalar.db.sql.statement.CreateCoordinatorTablesStatement;
import com.scalar.db.sql.statement.CreateIndexStatement;
import com.scalar.db.sql.statement.CreateNamespaceStatement;
import com.scalar.db.sql.statement.CreateTableStatement;
import com.scalar.db.sql.statement.DdlStatement;
import com.scalar.db.sql.statement.DdlStatementVisitor;
import com.scalar.db.sql.statement.DropCoordinatorTablesStatement;
import com.scalar.db.sql.statement.DropIndexStatement;
import com.scalar.db.sql.statement.DropNamespaceStatement;
import com.scalar.db.sql.statement.DropTableStatement;
import com.scalar.db.sql.statement.TruncateCoordinatorTablesStatement;
import com.scalar.db.sql.statement.TruncateTableStatement;
import java.util.Objects;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class DdlStatementExecutor implements DdlStatementVisitor<Void, Void> {

  private final DistributedTransactionAdmin admin;
  private final Metadata metadata;

  DdlStatementExecutor(DistributedTransactionAdmin admin, Metadata metadata) {
    this.admin = Objects.requireNonNull(admin);
    this.metadata = Objects.requireNonNull(metadata);
  }

  public void execute(DdlStatement statement) {
    statement.accept(this, null);
  }

  @Override
  public Void visit(CreateNamespaceStatement statement, Void context) {
    try {
      admin.createNamespace(statement.namespaceName, statement.ifNotExists, statement.options);
      return null;
    } catch (ExecutionException e) {
      throw new SqlException("Failed to create a namespace", e);
    }
  }

  @Override
  public Void visit(CreateTableStatement statement, Void context) {
    try {
      com.scalar.db.api.TableMetadata tableMetadata =
          convertCreateStatementToTableMetadata(statement);
      admin.createTable(
          statement.namespaceName,
          statement.tableName,
          tableMetadata,
          statement.ifNotExists,
          statement.options);
      return null;
    } catch (ExecutionException e) {
      throw new SqlException("Failed to create a table", e);
    } finally {
      // Invalidate the metadata cache
      metadata
          .getNamespace(statement.namespaceName)
          .filter(n -> n instanceof CachedNamespaceMetadata)
          .map(n -> (CachedNamespaceMetadata) n)
          .ifPresent(CachedNamespaceMetadata::invalidateTableNamesCache);
    }
  }

  private com.scalar.db.api.TableMetadata convertCreateStatementToTableMetadata(
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

  private com.scalar.db.io.DataType convertDataType(DataType dataType) {
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

  private Scan.Ordering.Order convertClusteringOrder(ClusteringOrder clusteringOrder) {
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
  public Void visit(DropNamespaceStatement statement, Void context) {
    try {
      if (statement.cascade) {
        for (String tableName : admin.getNamespaceTableNames(statement.namespaceName)) {
          admin.dropTable(statement.namespaceName, tableName);
        }
      }
      admin.dropNamespace(statement.namespaceName, statement.ifExists);
      return null;
    } catch (ExecutionException e) {
      throw new SqlException("Failed to drop a namespace", e);
    } finally {
      // Invalidate the metadata cache
      if (metadata instanceof CachedMetadata) {
        ((CachedMetadata) metadata).invalidateCache(statement.namespaceName);
      }
    }
  }

  @Override
  public Void visit(DropTableStatement statement, Void context) {
    try {
      admin.dropTable(statement.namespaceName, statement.tableName, statement.ifExists);
      return null;
    } catch (ExecutionException e) {
      throw new SqlException("Failed to drop a table", e);
    } finally {
      // Invalidate the metadata cache
      metadata
          .getNamespace(statement.namespaceName)
          .filter(n -> n instanceof CachedNamespaceMetadata)
          .map(n -> (CachedNamespaceMetadata) n)
          .ifPresent(
              n -> {
                n.invalidateTableNamesCache();
                n.invalidateTableMetadataCache(statement.tableName);
              });
    }
  }

  @Override
  public Void visit(TruncateTableStatement statement, Void context) {
    try {
      admin.truncateTable(statement.namespaceName, statement.tableName);
      return null;
    } catch (ExecutionException e) {
      throw new SqlException("Failed to drop a table", e);
    }
  }

  @Override
  public Void visit(CreateCoordinatorTablesStatement statement, Void context) {
    try {
      admin.createCoordinatorTables(statement.ifNotExist, statement.options);
      return null;
    } catch (ExecutionException e) {
      throw new SqlException("Failed to create a coordinator tables", e);
    }
  }

  @Override
  public Void visit(DropCoordinatorTablesStatement statement, Void context) {
    try {
      admin.dropCoordinatorTables(statement.ifExist);
      return null;
    } catch (ExecutionException e) {
      throw new SqlException("Failed to drop a coordinator tables", e);
    }
  }

  @Override
  public Void visit(TruncateCoordinatorTablesStatement statement, Void context) {
    try {
      admin.truncateCoordinatorTables();
      return null;
    } catch (ExecutionException e) {
      throw new SqlException("Failed to truncate a coordinator tables", e);
    }
  }

  @Override
  public Void visit(CreateIndexStatement statement, Void context) {
    try {
      admin.createIndex(
          statement.namespaceName,
          statement.tableName,
          statement.columnName,
          statement.ifNotExists,
          statement.options);
      return null;
    } catch (ExecutionException e) {
      throw new SqlException("Failed to create a index table", e);
    } finally {
      // Invalidate the metadata cache
      metadata
          .getNamespace(statement.namespaceName)
          .filter(n -> n instanceof CachedNamespaceMetadata)
          .map(n -> (CachedNamespaceMetadata) n)
          .ifPresent(n -> n.invalidateTableMetadataCache(statement.tableName));
    }
  }

  @Override
  public Void visit(DropIndexStatement statement, Void context) {
    try {
      admin.dropIndex(
          statement.namespaceName, statement.tableName, statement.columnName, statement.ifExists);
      return null;
    } catch (ExecutionException e) {
      throw new SqlException("Failed to drop a index table", e);
    } finally {
      // Invalidate the metadata cache
      metadata
          .getNamespace(statement.namespaceName)
          .filter(n -> n instanceof CachedNamespaceMetadata)
          .map(n -> (CachedNamespaceMetadata) n)
          .ifPresent(n -> n.invalidateTableMetadataCache(statement.tableName));
    }
  }
}
