package com.scalar.db.sql;

import com.google.common.collect.ImmutableList;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionAdmin;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Selection;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.TableMetadataManager;
import com.scalar.db.exception.transaction.AbortException;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.CrudConflictException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.sql.exception.SqlException;
import com.scalar.db.sql.exception.TransactionConflictException;
import com.scalar.db.sql.exception.UnknownTransactionStatusException;
import com.scalar.db.sql.statement.DdlStatement;
import com.scalar.db.sql.statement.DeleteStatement;
import com.scalar.db.sql.statement.DmlStatement;
import com.scalar.db.sql.statement.DmlStatementVisitor;
import com.scalar.db.sql.statement.InsertStatement;
import com.scalar.db.sql.statement.SelectStatement;
import com.scalar.db.sql.statement.Statement;
import com.scalar.db.sql.statement.UpdateStatement;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public class TransactionSqlSession implements SqlSession {

  private final DistributedTransactionAdmin admin;
  private final DistributedTransactionManager manager;
  private final TableMetadataManager tableMetadataManager;
  private final StatementValidator statementValidator;

  @Nullable private DistributedTransaction transaction;
  @Nullable private ResultSet resultSet;

  public TransactionSqlSession(
      DistributedTransactionAdmin admin,
      DistributedTransactionManager manager,
      TableMetadataManager tableMetadataManager) {
    this.admin = admin;
    this.manager = manager;
    this.tableMetadataManager = tableMetadataManager;
    statementValidator = new StatementValidator(tableMetadataManager);
  }

  @Override
  public void beginTransaction() {
    checkIfTransactionInProgress();

    try {
      transaction = manager.start();
      resultSet = null;
    } catch (TransactionException e) {
      throw new SqlException("Failed to begin a transaction", e);
    }
  }

  @Override
  public void beginTransaction(String transactionId) {
    checkIfTransactionInProgress();

    try {
      transaction = manager.start(transactionId);
      resultSet = null;
    } catch (TransactionException e) {
      throw new SqlException("Failed to begin a transaction", e);
    }
  }

  @Override
  public void joinTransaction(String transactionId) {
    throw new UnsupportedOperationException(
        "joining a transaction is not supported in transaction mode");
  }

  @Override
  public void execute(Statement statement) {
    statementValidator.validate(statement);

    if (statement instanceof DdlStatement) {
      checkIfTransactionInProgress();

      new DdlStatementExecutor(admin, (DdlStatement) statement).execute();
      resultSet = EmptyResultSet.INSTANCE;
    } else if (statement instanceof DmlStatement) {
      checkIfTransactionBegun();

      resultSet =
          new DmlStatementExecutor(transaction, tableMetadataManager, (DmlStatement) statement)
              .execute();
    } else {
      throw new AssertionError();
    }
  }

  @Nullable
  @Override
  public ResultSet getResultSet() {
    if (resultSet == null) {
      throw new IllegalStateException("No query executed yet");
    }

    return resultSet;
  }

  @Override
  public ResultSet executeQuery(SelectStatement statement) {
    checkIfTransactionBegun();

    statementValidator.validate(statement);
    resultSet = new DmlStatementExecutor(transaction, tableMetadataManager, statement).execute();
    return resultSet;
  }

  @Override
  public void prepare() {
    throw new UnsupportedOperationException(
        "preparing a transaction is not supported in transaction mode");
  }

  @Override
  public void validate() {
    throw new UnsupportedOperationException(
        "validating a transaction is not supported in transaction mode");
  }

  @Override
  public void commit() {
    checkIfTransactionBegun();
    assert transaction != null;

    try {
      transaction.commit();
      transaction = null;
      resultSet = null;
    } catch (CommitConflictException e) {
      throw new TransactionConflictException(
          "Conflict happened during committing a transaction", e);
    } catch (CommitException e) {
      throw new SqlException("Failed to commit a transaction", e);
    } catch (com.scalar.db.exception.transaction.UnknownTransactionStatusException e) {
      throw new UnknownTransactionStatusException("The transaction status is unknown", e);
    }
  }

  @Override
  public void rollback() {
    checkIfTransactionBegun();
    assert transaction != null;

    try {
      transaction.abort();
    } catch (AbortException e) {
      throw new SqlException("Failed to abort a transaction", e);
    } finally {
      transaction = null;
      resultSet = null;
    }
  }

  @Override
  public String getTransactionId() {
    checkIfTransactionBegun();
    assert transaction != null;
    return transaction.getId();
  }

  @Override
  public Metadata getMetadata() {
    checkIfTransactionInProgress();

    // TODO
    return null;
  }

  private void checkIfTransactionInProgress() {
    if (transaction != null) {
      throw new IllegalStateException(
          "The previous transaction is still in progress. Commit or rollback it first");
    }
  }

  private void checkIfTransactionBegun() {
    if (transaction == null) {
      throw new IllegalStateException("A transaction is not begun");
    }
  }

  private static class DmlStatementExecutor implements DmlStatementVisitor {

    private final DistributedTransaction transaction;
    private final TableMetadataManager tableMetadataManager;
    private final DmlStatement statement;

    private ResultSet resultSet;

    public DmlStatementExecutor(
        DistributedTransaction transaction,
        TableMetadataManager tableMetadataManager,
        DmlStatement statement) {
      this.transaction = transaction;
      this.tableMetadataManager = tableMetadataManager;
      this.statement = statement;
    }

    public ResultSet execute() {
      statement.accept(this);
      return resultSet;
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
                  .orElse(EmptyResultSet.INSTANCE);
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
        transaction.put(put);
        resultSet = EmptyResultSet.INSTANCE;
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
        transaction.put(put);
        resultSet = EmptyResultSet.INSTANCE;
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
        transaction.delete(delete);
        resultSet = EmptyResultSet.INSTANCE;
      } catch (CrudConflictException e) {
        throw new TransactionConflictException("Conflict happened during deleting a record", e);
      } catch (CrudException e) {
        throw new SqlException("Failed to delete a record", e);
      }
    }
  }
}
