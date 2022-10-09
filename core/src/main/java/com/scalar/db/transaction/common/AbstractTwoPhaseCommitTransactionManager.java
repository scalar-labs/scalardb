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
import java.util.List;
import java.util.Optional;
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
      throw new IllegalStateException(
          "The transaction already exists. transactionId: " + transaction.getId());
    }
  }

  private void removeActiveTransaction(String transactionId) {
    activeTransactions.remove(transactionId);
  }

  @Override
  public TwoPhaseCommitTransaction resume(String txId) {
    return activeTransactions
        .get(txId)
        .orElseThrow(
            () ->
                new IllegalStateException(
                    "A transaction associated with the specified transaction ID is not found. "
                        + "It might have been expired. transactionId: "
                        + txId));
  }

  @VisibleForTesting
  public class ActiveTransaction extends AbstractTwoPhaseCommitTransaction {

    private final TwoPhaseCommitTransaction transaction;

    @SuppressFBWarnings("EI_EXPOSE_REP2")
    public ActiveTransaction(TwoPhaseCommitTransaction transaction) throws TransactionException {
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

    @VisibleForTesting
    public TwoPhaseCommitTransaction getActualTransaction() {
      return transaction;
    }
  }
}
