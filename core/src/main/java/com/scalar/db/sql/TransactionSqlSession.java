package com.scalar.db.sql;

import com.google.common.annotations.VisibleForTesting;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionAdmin;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.common.TableMetadataManager;
import com.scalar.db.exception.transaction.AbortException;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.sql.exception.SqlException;
import com.scalar.db.sql.exception.TransactionConflictException;
import com.scalar.db.sql.exception.UnknownTransactionStatusException;
import com.scalar.db.sql.statement.DdlStatement;
import com.scalar.db.sql.statement.DmlStatement;
import com.scalar.db.sql.statement.SelectStatement;
import com.scalar.db.sql.statement.Statement;
import java.util.Objects;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public class TransactionSqlSession implements SqlSession {

  private final DistributedTransactionAdmin admin;
  private final DistributedTransactionManager manager;
  private final TableMetadataManager tableMetadataManager;
  private final StatementValidator statementValidator;
  private final DdlStatementExecutor ddlStatementExecutor;

  @Nullable private DistributedTransaction transaction;
  @Nullable private ResultSet resultSet;

  TransactionSqlSession(
      DistributedTransactionAdmin admin,
      DistributedTransactionManager manager,
      TableMetadataManager tableMetadataManager) {
    this.admin = Objects.requireNonNull(admin);
    this.manager = Objects.requireNonNull(manager);
    this.tableMetadataManager = Objects.requireNonNull(tableMetadataManager);
    statementValidator = new StatementValidator(tableMetadataManager);
    ddlStatementExecutor = new DdlStatementExecutor(admin);
  }

  @VisibleForTesting
  TransactionSqlSession(
      DistributedTransactionAdmin admin,
      DistributedTransactionManager manager,
      TableMetadataManager tableMetadataManager,
      StatementValidator statementValidator,
      DdlStatementExecutor ddlStatementExecutor) {
    this.admin = Objects.requireNonNull(admin);
    this.manager = Objects.requireNonNull(manager);
    this.tableMetadataManager = Objects.requireNonNull(tableMetadataManager);
    this.statementValidator = Objects.requireNonNull(statementValidator);
    this.ddlStatementExecutor = Objects.requireNonNull(ddlStatementExecutor);
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
  public void joinTransaction(String transactionId) {
    throw new UnsupportedOperationException(
        "Joining a transaction is not supported in transaction mode");
  }

  @Override
  public void execute(Statement statement) {
    if (statement instanceof DdlStatement) {
      checkIfTransactionInProgress();

      statementValidator.validate(statement);
      ddlStatementExecutor.execute((DdlStatement) statement);
      resultSet = EmptyResultSet.INSTANCE;
    } else if (statement instanceof DmlStatement) {
      checkIfTransactionBegun();

      statementValidator.validate(statement);
      resultSet = executeDmlStatement((DmlStatement) statement);
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
    resultSet = executeDmlStatement(statement);
    return resultSet;
  }

  @VisibleForTesting
  ResultSet executeDmlStatement(DmlStatement statement) {
    return new DmlStatementExecutor(transaction, tableMetadataManager, statement).execute();
  }

  @Override
  public void prepare() {
    throw new UnsupportedOperationException(
        "Preparing a transaction is not supported in transaction mode");
  }

  @Override
  public void validate() {
    throw new UnsupportedOperationException(
        "Validating a transaction is not supported in transaction mode");
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
    return new Metadata(admin);
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
}
