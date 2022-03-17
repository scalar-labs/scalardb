package com.scalar.db.sql;

import com.scalar.db.api.DistributedTransactionAdmin;
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
      TableMetadata tableMetadata = SqlUtils.convertCreateStatementToTableMetadata(statement);
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
