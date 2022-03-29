package com.scalar.db.sql;

import com.google.common.annotations.VisibleForTesting;
import com.scalar.db.api.DistributedTransactionAdmin;
import com.scalar.db.api.TwoPhaseCommitTransaction;
import com.scalar.db.api.TwoPhaseCommitTransactionManager;
import com.scalar.db.common.TableMetadataManager;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.PreparationConflictException;
import com.scalar.db.exception.transaction.PreparationException;
import com.scalar.db.exception.transaction.RollbackException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.exception.transaction.ValidationConflictException;
import com.scalar.db.exception.transaction.ValidationException;
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
public class TwoPhaseCommitTransactionSqlSession implements SqlSession {

  private final DistributedTransactionAdmin admin;
  private final TwoPhaseCommitTransactionManager manager;
  private final StatementValidator statementValidator;
  private final DmlStatementExecutor dmlStatementExecutor;
  private final DdlStatementExecutor ddlStatementExecutor;

  @Nullable private TwoPhaseCommitTransaction transaction;
  @Nullable private ResultSet resultSet;

  TwoPhaseCommitTransactionSqlSession(
      DistributedTransactionAdmin admin,
      TwoPhaseCommitTransactionManager manager,
      TableMetadataManager tableMetadataManager) {
    this(admin, manager, null, tableMetadataManager);
  }

  TwoPhaseCommitTransactionSqlSession(
      DistributedTransactionAdmin admin,
      TwoPhaseCommitTransactionManager manager,
      @Nullable TwoPhaseCommitTransaction transaction,
      TableMetadataManager tableMetadataManager) {
    this.admin = Objects.requireNonNull(admin);
    this.manager = Objects.requireNonNull(manager);
    this.transaction = transaction;
    statementValidator = new StatementValidator(tableMetadataManager);
    dmlStatementExecutor = new DmlStatementExecutor(tableMetadataManager);
    ddlStatementExecutor = new DdlStatementExecutor(admin);
  }

  @VisibleForTesting
  TwoPhaseCommitTransactionSqlSession(
      DistributedTransactionAdmin admin,
      TwoPhaseCommitTransactionManager manager,
      StatementValidator statementValidator,
      DmlStatementExecutor dmlStatementExecutor,
      DdlStatementExecutor ddlStatementExecutor) {
    this.admin = Objects.requireNonNull(admin);
    this.manager = Objects.requireNonNull(manager);
    this.statementValidator = Objects.requireNonNull(statementValidator);
    this.dmlStatementExecutor = Objects.requireNonNull(dmlStatementExecutor);
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
    checkIfTransactionInProgress();

    try {
      transaction = manager.join(transactionId);
      resultSet = null;
    } catch (TransactionException e) {
      throw new SqlException("Failed to begin a transaction", e);
    }
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
      resultSet = dmlStatementExecutor.execute(transaction, (DmlStatement) statement);
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
    resultSet = dmlStatementExecutor.execute(transaction, statement);
    return resultSet;
  }

  @Override
  public void prepare() {
    checkIfTransactionBegun();
    assert transaction != null;

    try {
      transaction.prepare();
    } catch (PreparationConflictException e) {
      throw new TransactionConflictException("Conflict happened during preparing a transaction", e);
    } catch (PreparationException e) {
      throw new SqlException("Failed to prepare a transaction", e);
    }
  }

  @Override
  public void validate() {
    checkIfTransactionBegun();
    assert transaction != null;

    try {
      transaction.validate();
    } catch (ValidationConflictException e) {
      throw new TransactionConflictException(
          "Conflict happened during validating a transaction", e);
    } catch (ValidationException e) {
      throw new SqlException("Failed to validate a transaction", e);
    }
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
      transaction.rollback();
    } catch (RollbackException e) {
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
