package com.scalar.db.transaction.common;

import com.google.common.annotations.VisibleForTesting;
import com.scalar.db.api.Delete;
import com.scalar.db.api.Get;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.TwoPhaseCommitTransaction;
import com.scalar.db.api.TwoPhaseCommitTransactionManager;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.PreparationException;
import com.scalar.db.exception.transaction.RollbackException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.exception.transaction.ValidationException;
import com.scalar.db.util.ActiveExpiringMap;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractTwoPhaseCommitTransactionManager
    implements TwoPhaseCommitTransactionManager {

  private static final long TRANSACTION_EXPIRATION_INTERVAL_MILLIS = 1000;

  private static final Logger logger =
      LoggerFactory.getLogger(AbstractTwoPhaseCommitTransactionManager.class);

  private Optional<String> namespace;
  private Optional<String> tableName;

  private final ActiveExpiringMap<String, TwoPhaseCommitTransaction> activeTransactions;

  public AbstractTwoPhaseCommitTransactionManager(DatabaseConfig config) {
    namespace = Optional.empty();
    tableName = Optional.empty();
    activeTransactions =
        new ActiveExpiringMap<>(
            config.getActiveTransactionManagementExpirationTimeMillis(),
            TRANSACTION_EXPIRATION_INTERVAL_MILLIS,
            t -> {
              logger.warn("the transaction is expired. transactionId: {}", t.getId());
              try {
                t.rollback();
              } catch (RollbackException e) {
                logger.warn("rollback failed", e);
              }
            });
  }

  /** @deprecated As of release 3.6.0. Will be removed in release 5.0.0 */
  @Deprecated
  @Override
  public void with(String namespace, String tableName) {
    this.namespace = Optional.ofNullable(namespace);
    this.tableName = Optional.ofNullable(tableName);
  }

  /** @deprecated As of release 3.6.0. Will be removed in release 5.0.0 */
  @Deprecated
  @Override
  public void withNamespace(String namespace) {
    this.namespace = Optional.ofNullable(namespace);
  }

  /** @deprecated As of release 3.6.0. Will be removed in release 5.0.0 */
  @Deprecated
  @Override
  public Optional<String> getNamespace() {
    return namespace;
  }

  /** @deprecated As of release 3.6.0. Will be removed in release 5.0.0 */
  @Deprecated
  @Override
  public void withTable(String tableName) {
    this.tableName = Optional.ofNullable(tableName);
  }

  /** @deprecated As of release 3.6.0. Will be removed in release 5.0.0 */
  @Deprecated
  @Override
  public Optional<String> getTable() {
    return tableName;
  }

  private void addActiveTransaction(ActiveTransaction transaction) throws TransactionException {
    if (activeTransactions.putIfAbsent(transaction.getId(), transaction) != null) {
      transaction.rollback();
      throw new TransactionException("The transaction already exists");
    }
  }

  private void removeActiveTransaction(String transactionId) {
    activeTransactions.remove(transactionId);
  }

  @Override
  public TwoPhaseCommitTransaction resume(String txId) throws TransactionException {
    return activeTransactions
        .get(txId)
        .orElseThrow(
            () ->
                new TransactionException(
                    "A transaction associated with the specified transaction ID is not found. "
                        + "It might have been expired"));
  }

  protected TwoPhaseCommitTransaction wrap(TwoPhaseCommitTransaction transaction)
      throws TransactionException {
    return new ActiveTransaction(new StateManagedTransaction(transaction));
  }

  private class ActiveTransaction extends AbstractTwoPhaseCommitTransaction
      implements WrappedTwoPhaseCommitTransaction {

    private final TwoPhaseCommitTransaction transaction;

    @SuppressFBWarnings("EI_EXPOSE_REP2")
    private ActiveTransaction(TwoPhaseCommitTransaction transaction) throws TransactionException {
      this.transaction = transaction;
      addActiveTransaction(this);
    }

    @Override
    public String getId() {
      return transaction.getId();
    }

    @Override
    public Optional<Result> get(Get get) throws CrudException {
      return transaction.get(get);
    }

    @Override
    public List<Result> scan(Scan scan) throws CrudException {
      return transaction.scan(scan);
    }

    @Override
    public void put(Put put) throws CrudException {
      transaction.put(put);
    }

    @Override
    public void put(List<Put> puts) throws CrudException {
      transaction.put(puts);
    }

    @Override
    public void delete(Delete delete) throws CrudException {
      transaction.delete(delete);
    }

    @Override
    public void delete(List<Delete> deletes) throws CrudException {
      transaction.delete(deletes);
    }

    @Override
    public void mutate(List<? extends Mutation> mutations) throws CrudException {
      transaction.mutate(mutations);
    }

    @Override
    public void prepare() throws PreparationException {
      transaction.prepare();
    }

    @Override
    public void validate() throws ValidationException {
      transaction.validate();
    }

    @Override
    public void commit() throws CommitException, UnknownTransactionStatusException {
      transaction.commit();
      removeActiveTransaction(getId());
    }

    @Override
    public void rollback() throws RollbackException {
      try {
        transaction.rollback();
      } finally {
        removeActiveTransaction(getId());
      }
    }

    @Override
    public TwoPhaseCommitTransaction getOriginalTransaction() {
      if (transaction instanceof WrappedTwoPhaseCommitTransaction) {
        return ((WrappedTwoPhaseCommitTransaction) transaction).getOriginalTransaction();
      }
      return transaction;
    }
  }

  /**
   * This class is to unify the call sequence of the transaction object. It doesn't care about the
   * potential inconsistency between the status field on JVM memory and the underlying persistent
   * layer.
   */
  @VisibleForTesting
  static class StateManagedTransaction extends AbstractTwoPhaseCommitTransaction
      implements WrappedTwoPhaseCommitTransaction {

    private enum Status {
      ACTIVE,
      PREPARED,
      PREPARE_FAILED,
      VALIDATED,
      VALIDATION_FAILED,
      COMMITTED,
      COMMIT_FAILED,
      ROLLED_BACK
    }

    private final TwoPhaseCommitTransaction transaction;
    private Status status;

    @VisibleForTesting
    StateManagedTransaction(TwoPhaseCommitTransaction transaction) {
      this.transaction = transaction;
      status = Status.ACTIVE;
    }

    @Override
    public String getId() {
      return transaction.getId();
    }

    @Override
    public Optional<Result> get(Get get) throws CrudException {
      checkStatus("The transaction is not active", Status.ACTIVE);
      return transaction.get(get);
    }

    @Override
    public List<Result> scan(Scan scan) throws CrudException {
      checkStatus("The transaction is not active", Status.ACTIVE);
      return transaction.scan(scan);
    }

    @Override
    public void put(Put put) throws CrudException {
      checkStatus("The transaction is not active", Status.ACTIVE);
      transaction.put(put);
    }

    @Override
    public void put(List<Put> puts) throws CrudException {
      checkStatus("The transaction is not active", Status.ACTIVE);
      transaction.put(puts);
    }

    @Override
    public void delete(Delete delete) throws CrudException {
      checkStatus("The transaction is not active", Status.ACTIVE);
      transaction.delete(delete);
    }

    @Override
    public void delete(List<Delete> deletes) throws CrudException {
      checkStatus("The transaction is not active", Status.ACTIVE);
      transaction.delete(deletes);
    }

    @Override
    public void mutate(List<? extends Mutation> mutations) throws CrudException {
      checkStatus("The transaction is not active", Status.ACTIVE);
      transaction.mutate(mutations);
    }

    @Override
    public void prepare() throws PreparationException {
      checkStatus("The transaction is not active", Status.ACTIVE);
      try {
        transaction.prepare();
        status = Status.PREPARED;
      } catch (Exception e) {
        status = Status.PREPARE_FAILED;
        throw e;
      }
    }

    @Override
    public void validate() throws ValidationException {
      checkStatus("The transaction is not prepared", Status.PREPARED);
      try {
        transaction.validate();
        status = Status.VALIDATED;
      } catch (Exception e) {
        status = Status.VALIDATION_FAILED;
        throw e;
      }
    }

    @Override
    public void commit() throws CommitException, UnknownTransactionStatusException {
      checkStatus(
          "The transaction is not prepared or validated.", Status.PREPARED, Status.VALIDATED);
      try {
        transaction.commit();
        status = Status.COMMITTED;
      } catch (Exception e) {
        status = Status.COMMIT_FAILED;
        throw e;
      }
    }

    @Override
    public void rollback() throws RollbackException {
      if (status == Status.COMMITTED || status == Status.ROLLED_BACK) {
        throw new IllegalStateException(
            "The transaction has already been committed or rolled back");
      }
      try {
        transaction.rollback();
      } finally {
        status = Status.ROLLED_BACK;
      }
    }

    private void checkStatus(@Nullable String message, Status... expectedStatus) {
      boolean expected = Arrays.stream(expectedStatus).anyMatch(s -> status == s);
      if (!expected) {
        throw new IllegalStateException(message);
      }
    }

    @Override
    public TwoPhaseCommitTransaction getOriginalTransaction() {
      if (transaction instanceof WrappedTwoPhaseCommitTransaction) {
        return ((WrappedTwoPhaseCommitTransaction) transaction).getOriginalTransaction();
      }
      return transaction;
    }
  }
}
