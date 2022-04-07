package com.scalar.db.sql;

import com.google.common.annotations.VisibleForTesting;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionAdmin;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.exception.transaction.AbortException;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.sql.exception.SqlException;
import com.scalar.db.sql.exception.TransactionConflictException;
import com.scalar.db.sql.exception.UnknownTransactionStatusException;
import com.scalar.db.sql.metadata.Metadata;
import com.scalar.db.sql.statement.DdlStatement;
import com.scalar.db.sql.statement.DmlStatement;
import com.scalar.db.sql.statement.Statement;
import java.util.Objects;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public class TransactionSession implements SqlStatementSession {

  private final DistributedTransactionManager manager;
  private final Metadata metadata;
  private final StatementValidator statementValidator;
  private final DmlStatementExecutor dmlStatementExecutor;
  private final DdlStatementExecutor ddlStatementExecutor;

  @Nullable private DistributedTransaction transaction;

  TransactionSession(
      DistributedTransactionAdmin admin, DistributedTransactionManager manager, Metadata metadata) {
    this.manager = Objects.requireNonNull(manager);
    this.metadata = Objects.requireNonNull(metadata);
    statementValidator = new StatementValidator(metadata);
    dmlStatementExecutor = new DmlStatementExecutor(metadata);
    ddlStatementExecutor = new DdlStatementExecutor(admin);
  }

  @VisibleForTesting
  TransactionSession(
      DistributedTransactionManager manager,
      Metadata metadata,
      StatementValidator statementValidator,
      DmlStatementExecutor dmlStatementExecutor,
      DdlStatementExecutor ddlStatementExecutor) {
    this.manager = Objects.requireNonNull(manager);
    this.metadata = Objects.requireNonNull(metadata);
    this.statementValidator = Objects.requireNonNull(statementValidator);
    this.dmlStatementExecutor = Objects.requireNonNull(dmlStatementExecutor);
    this.ddlStatementExecutor = Objects.requireNonNull(ddlStatementExecutor);
  }

  @Override
  public void begin() {
    checkIfTransactionInProgress();

    try {
      transaction = manager.start();
    } catch (TransactionException e) {
      throw new SqlException("Failed to begin a transaction", e);
    }
  }

  @Override
  public void join(String transactionId) {
    throw new UnsupportedOperationException(
        "Joining a transaction is not supported in transaction mode");
  }

  @Override
  public void resume(String transactionId) {
    throw new UnsupportedOperationException(
        "Resuming a transaction is not supported in transaction mode");
  }

  @Override
  public ResultSet execute(Statement statement) {
    if (statement instanceof DdlStatement) {
      checkIfTransactionInProgress();

      statementValidator.validate(statement);
      ddlStatementExecutor.execute((DdlStatement) statement);
      return EmptyResultSet.INSTANCE;
    } else if (statement instanceof DmlStatement) {
      checkIfTransactionBegun();

      statementValidator.validate(statement);
      return dmlStatementExecutor.execute(transaction, (DmlStatement) statement);
    } else {
      throw new AssertionError();
    }
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
    }
  }

  @Override
  public String getTransactionId() {
    checkIfTransactionBegun();
    assert transaction != null;
    return transaction.getId();
  }

  @Override
  public boolean isTransactionInProgress() {
    return transaction != null;
  }

  @Override
  public Metadata getMetadata() {
    checkIfTransactionInProgress();
    return metadata;
  }

  private void checkIfTransactionInProgress() {
    if (isTransactionInProgress()) {
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
