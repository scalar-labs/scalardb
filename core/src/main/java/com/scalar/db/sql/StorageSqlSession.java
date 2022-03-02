package com.scalar.db.sql;

import com.google.common.collect.ImmutableList;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.Get;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scanner;
import com.scalar.db.api.Selection;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.storage.NoMutationException;
import com.scalar.db.exception.storage.RetriableExecutionException;
import com.scalar.db.sql.exception.RetriableSqlException;
import com.scalar.db.sql.exception.SqlException;
import com.scalar.db.sql.statement.BatchStatement;
import com.scalar.db.sql.statement.BatchableStatementVisitor;
import com.scalar.db.sql.statement.CreateNamespaceStatement;
import com.scalar.db.sql.statement.CreateTableStatement;
import com.scalar.db.sql.statement.DeleteStatement;
import com.scalar.db.sql.statement.DropNamespaceStatement;
import com.scalar.db.sql.statement.DropTableStatement;
import com.scalar.db.sql.statement.InsertStatement;
import com.scalar.db.sql.statement.SelectStatement;
import com.scalar.db.sql.statement.Statement;
import com.scalar.db.sql.statement.StatementVisitor;
import com.scalar.db.sql.statement.TruncateTableStatement;
import com.scalar.db.sql.statement.UpdateStatement;
import com.scalar.db.util.TableMetadataManager;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class StorageSqlSession implements SqlSession {

  private final DistributedStorage storage;
  private final DistributedStorageAdmin admin;
  private final TableMetadataManager tableMetadataManager;

  public StorageSqlSession(
      DistributedStorage storage,
      DistributedStorageAdmin admin,
      TableMetadataManager tableMetadataManager) {
    this.storage = storage;
    this.admin = admin;
    this.tableMetadataManager = tableMetadataManager;
  }

  @Override
  public ResultSet execute(Statement statement) {
    new StatementValidator(tableMetadataManager, statement).validate();
    return new StatementExecutor(storage, admin, tableMetadataManager, statement).execute();
  }

  private static class StatementExecutor implements StatementVisitor {

    private final DistributedStorage storage;
    private final DistributedStorageAdmin admin;
    private final TableMetadataManager tableMetadataManager;
    private final Statement statement;

    private ResultSet resultSet;

    public StatementExecutor(
        DistributedStorage storage,
        DistributedStorageAdmin admin,
        TableMetadataManager tableMetadataManager,
        Statement statement) {
      this.storage = storage;
      this.admin = admin;
      this.tableMetadataManager = tableMetadataManager;
      this.statement = statement;
    }

    public ResultSet execute() {
      statement.accept(this);
      return resultSet;
    }

    @Override
    public void visit(CreateNamespaceStatement statement) {
      try {
        admin.createNamespace(statement.namespaceName, statement.ifNotExists, statement.options);
        resultSet = new EmptyResultSet();
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
        resultSet = new EmptyResultSet();
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
        resultSet = new EmptyResultSet();
      } catch (ExecutionException e) {
        throw new SqlException("Failed to drop a namespace", e);
      }
    }

    @Override
    public void visit(DropTableStatement statement) {
      try {
        admin.dropTable(statement.namespaceName, statement.tableName, statement.ifExists);
        resultSet = new EmptyResultSet();
      } catch (ExecutionException e) {
        throw new SqlException("Failed to drop a table", e);
      }
    }

    @Override
    public void visit(TruncateTableStatement statement) {
      try {
        admin.truncateTable(statement.namespaceName, statement.tableName);
        resultSet = new EmptyResultSet();
      } catch (ExecutionException e) {
        throw new SqlException("Failed to drop a table", e);
      }
    }

    @Override
    public void visit(SelectStatement statement) {
      TableMetadata metadata =
          SqlUtils.getTableMetadata(
              tableMetadataManager, statement.namespaceName, statement.tableName);

      Selection selection = SqlUtils.convertSelectStatementToSelection(statement, metadata);

      ImmutableList<String> projectedColumnNames =
          statement.projectedColumnNames.isEmpty()
              ? ImmutableList.copyOf(metadata.getColumnNames())
              : statement.projectedColumnNames;

      try {
        if (selection instanceof Get) {
          Optional<Result> result = storage.get((Get) selection);
          resultSet =
              result
                  .map(
                      r ->
                          (ResultSet)
                              new SingleRecordResultSet(new ResultRecord(r, projectedColumnNames)))
                  .orElse(new EmptyResultSet());
        } else {
          Scanner scanner = storage.scan((Scan) selection);
          resultSet = new ScannerResultSet(scanner, projectedColumnNames);
        }
      } catch (ExecutionException e) {
        throw new SqlException("Failed to drop a table", e);
      }
    }

    @Override
    public void visit(InsertStatement statement) {
      TableMetadata metadata =
          SqlUtils.getTableMetadata(
              tableMetadataManager, statement.namespaceName, statement.tableName);
      Put put = SqlUtils.convertInsertStatementToPut(statement, metadata);
      try {
        storage.put(put);
        if (put.getCondition().isPresent()) {
          resultSet = new SingleRecordResultSet(new ConditionalMutationResultRecord(true));
        } else {
          resultSet = new EmptyResultSet();
        }
      } catch (NoMutationException e) {
        resultSet = new SingleRecordResultSet(new ConditionalMutationResultRecord(false));
      } catch (RetriableExecutionException e) {
        throw new RetriableSqlException("Failed to insert a record. You can retry in this case", e);
      } catch (ExecutionException e) {
        throw new SqlException("Failed to insert a record", e);
      }
    }

    @Override
    public void visit(UpdateStatement statement) {
      TableMetadata metadata =
          SqlUtils.getTableMetadata(
              tableMetadataManager, statement.namespaceName, statement.tableName);
      Put put = SqlUtils.convertUpdateStatementToPut(statement, metadata);
      try {
        storage.put(put);
        if (put.getCondition().isPresent()) {
          resultSet = new SingleRecordResultSet(new ConditionalMutationResultRecord(true));
        } else {
          resultSet = new EmptyResultSet();
        }
      } catch (NoMutationException e) {
        resultSet = new SingleRecordResultSet(new ConditionalMutationResultRecord(false));
      } catch (RetriableExecutionException e) {
        throw new RetriableSqlException("Failed to update a record. You can retry in this case", e);
      } catch (ExecutionException e) {
        throw new SqlException("Failed to update a record", e);
      }
    }

    @Override
    public void visit(DeleteStatement statement) {
      TableMetadata metadata =
          SqlUtils.getTableMetadata(
              tableMetadataManager, statement.namespaceName, statement.tableName);
      Delete delete = SqlUtils.convertDeleteStatementToDelete(statement, metadata);
      try {
        storage.delete(delete);
        if (delete.getCondition().isPresent()) {
          resultSet = new SingleRecordResultSet(new ConditionalMutationResultRecord(true));
        } else {
          resultSet = new EmptyResultSet();
        }
      } catch (NoMutationException e) {
        resultSet = new SingleRecordResultSet(new ConditionalMutationResultRecord(false));
      } catch (RetriableExecutionException e) {
        throw new RetriableSqlException("Failed to delete a record. You can retry in this case", e);
      } catch (ExecutionException e) {
        throw new SqlException("Failed to delete a record", e);
      }
    }

    @Override
    public void visit(BatchStatement statement) {
      resultSet = new BatchStatementExecutor(storage, tableMetadataManager, statement).execute();
    }
  }

  private static class BatchStatementExecutor implements BatchableStatementVisitor {

    private final DistributedStorage storage;
    private final TableMetadataManager tableMetadataManager;
    private final BatchStatement statement;
    private final List<Mutation> mutations;

    public BatchStatementExecutor(
        DistributedStorage storage,
        TableMetadataManager tableMetadataManager,
        BatchStatement statement) {
      this.storage = storage;
      this.tableMetadataManager = tableMetadataManager;
      this.statement = statement;

      mutations = new ArrayList<>(statement.statements.size());
    }

    public ResultSet execute() {
      statement.statements.forEach(s -> s.accept(this));
      boolean hasCondition = mutations.stream().anyMatch(m -> m.getCondition().isPresent());

      ResultSet resultSet;
      try {
        storage.mutate(mutations);
        if (hasCondition) {
          resultSet = new SingleRecordResultSet(new ConditionalMutationResultRecord(true));
        } else {
          resultSet = new EmptyResultSet();
        }
      } catch (NoMutationException e) {
        resultSet = new SingleRecordResultSet(new ConditionalMutationResultRecord(false));
      } catch (RetriableExecutionException e) {
        throw new RetriableSqlException("Failed to mutate records. You can retry in this case", e);
      } catch (ExecutionException e) {
        throw new SqlException("Failed to mutate records", e);
      }
      return resultSet;
    }

    @Override
    public void visit(InsertStatement statement) {
      TableMetadata metadata =
          SqlUtils.getTableMetadata(
              tableMetadataManager, statement.namespaceName, statement.tableName);
      mutations.add(SqlUtils.convertInsertStatementToPut(statement, metadata));
    }

    @Override
    public void visit(UpdateStatement statement) {
      TableMetadata metadata =
          SqlUtils.getTableMetadata(
              tableMetadataManager, statement.namespaceName, statement.tableName);
      mutations.add(SqlUtils.convertUpdateStatementToPut(statement, metadata));
    }

    @Override
    public void visit(DeleteStatement statement) {
      TableMetadata metadata =
          SqlUtils.getTableMetadata(
              tableMetadataManager, statement.namespaceName, statement.tableName);
      mutations.add(SqlUtils.convertDeleteStatementToDelete(statement, metadata));
    }
  }
}
