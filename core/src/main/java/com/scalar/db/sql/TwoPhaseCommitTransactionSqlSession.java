package com.scalar.db.sql;

import com.google.common.collect.ImmutableList;
import com.scalar.db.api.Delete;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Selection;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.api.TwoPhaseCommitTransaction;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.CrudConflictException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.PreparationConflictException;
import com.scalar.db.exception.transaction.PreparationException;
import com.scalar.db.exception.transaction.RollbackException;
import com.scalar.db.exception.transaction.ValidationConflictException;
import com.scalar.db.exception.transaction.ValidationException;
import com.scalar.db.sql.exception.SqlException;
import com.scalar.db.sql.exception.TransactionConflictException;
import com.scalar.db.sql.exception.UnknownTransactionStatusException;
import com.scalar.db.sql.statement.BatchStatement;
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
import java.util.List;
import java.util.Optional;

public class TwoPhaseCommitTransactionSqlSession implements SqlSession {
  private final TwoPhaseCommitTransaction transaction;
  private final TableMetadataManager tableMetadataManager;

  public TwoPhaseCommitTransactionSqlSession(
      TwoPhaseCommitTransaction transaction, TableMetadataManager tableMetadataManager) {
    this.transaction = transaction;
    this.tableMetadataManager = tableMetadataManager;
  }

  @Override
  public ResultSet execute(Statement statement) {
    new StatementValidator(tableMetadataManager, statement).validate();
    return new StatementExecutor(transaction, tableMetadataManager, statement).execute();
  }

  public String getTransactionId() {
    return transaction.getId();
  }

  public void prepare() {
    try {
      transaction.prepare();
    } catch (PreparationConflictException e) {
      throw new TransactionConflictException("Conflict happened during preparing a transaction", e);
    } catch (PreparationException e) {
      throw new SqlException("Failed to prepare a transaction", e);
    }
  }

  public void validation() {
    try {
      transaction.validate();
    } catch (ValidationConflictException e) {
      throw new TransactionConflictException(
          "Conflict happened during validating a transaction", e);
    } catch (ValidationException e) {
      throw new SqlException("Failed to validate a transaction", e);
    }
  }

  public void commit() {
    try {
      transaction.commit();
    } catch (CommitConflictException e) {
      throw new TransactionConflictException(
          "Conflict happened during committing a transaction", e);
    } catch (CommitException e) {
      throw new SqlException("Failed to commit a transaction", e);
    } catch (com.scalar.db.exception.transaction.UnknownTransactionStatusException e) {
      throw new UnknownTransactionStatusException("The transaction status is unknown", e);
    }
  }

  public void rollback() {
    try {
      transaction.rollback();
    } catch (RollbackException e) {
      throw new SqlException("Failed to abort a transaction", e);
    }
  }

  private static class StatementExecutor implements StatementVisitor {

    private final TwoPhaseCommitTransaction transaction;
    private final TableMetadataManager tableMetadataManager;
    private final Statement statement;

    private ResultSet resultSet;

    public StatementExecutor(
        TwoPhaseCommitTransaction transaction,
        TableMetadataManager tableMetadataManager,
        Statement statement) {
      this.transaction = transaction;
      this.tableMetadataManager = tableMetadataManager;
      this.statement = statement;
    }

    public ResultSet execute() {
      statement.accept(this);
      return resultSet;
    }

    @Override
    public void visit(CreateNamespaceStatement statement) {
      throw new UnsupportedOperationException(
          "Creating a namespace is not supported in two-phase commit transaction mode");
    }

    @Override
    public void visit(CreateTableStatement statement) {
      throw new UnsupportedOperationException(
          "Creating a table is not supported in two-phase commit transaction mode");
    }

    @Override
    public void visit(DropNamespaceStatement statement) {
      throw new UnsupportedOperationException(
          "Dropping a namespace is not supported in two-phase commit transaction mode");
    }

    @Override
    public void visit(DropTableStatement statement) {
      throw new UnsupportedOperationException(
          "Dropping a table is not supported in two-phase commit transaction mode");
    }

    @Override
    public void visit(TruncateTableStatement statement) {
      throw new UnsupportedOperationException(
          "Truncating a table is not supported in two-phase commit transaction mode");
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
          Optional<Result> result = transaction.get((Get) selection);
          resultSet =
              result
                  .map(
                      r ->
                          (ResultSet)
                              new SingleRecordResultSet(new ResultRecord(r, projectedColumnNames)))
                  .orElse(new EmptyResultSet());
        } else {
          List<Result> results = transaction.scan((Scan) selection);
          resultSet = new ResultIteratorResultSet(results.iterator(), projectedColumnNames);
        }
      } catch (CrudConflictException e) {
        throw new TransactionConflictException("Conflict happened during selecting a record", e);
      } catch (CrudException e) {
        throw new SqlException("Failed to insert a record", e);
      }
    }

    @Override
    public void visit(InsertStatement statement) {
      TableMetadata metadata =
          SqlUtils.getTableMetadata(
              tableMetadataManager, statement.namespaceName, statement.tableName);
      Put put = SqlUtils.convertInsertStatementToPut(statement, metadata);
      try {
        if (put.getCondition().isPresent()) {
          throw new UnsupportedOperationException(
              "Conditional update is not supported in transaction mode");
        }
        transaction.put(put);
        resultSet = new EmptyResultSet();
      } catch (CrudConflictException e) {
        throw new TransactionConflictException("Conflict happened during inserting a record", e);
      } catch (CrudException e) {
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
        if (put.getCondition().isPresent()) {
          throw new UnsupportedOperationException(
              "Conditional update is not supported in transaction mode");
        }
        transaction.put(put);
        resultSet = new EmptyResultSet();
      } catch (CrudConflictException e) {
        throw new TransactionConflictException("Conflict happened during updating a record", e);
      } catch (CrudException e) {
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
        if (delete.getCondition().isPresent()) {
          throw new UnsupportedOperationException(
              "Conditional update is not supported in transaction mode");
        }
        transaction.delete(delete);
        resultSet = new EmptyResultSet();
      } catch (CrudConflictException e) {
        throw new TransactionConflictException("Conflict happened during deleting a record", e);
      } catch (CrudException e) {
        throw new SqlException("Failed to delete a record", e);
      }
    }

    @Override
    public void visit(BatchStatement statement) {
      throw new UnsupportedOperationException(
          "Batch is not supported in two-phase commit transaction mode");
    }
  }
}
