package com.scalar.db.common;

import com.scalar.db.api.Delete;
import com.scalar.db.api.Get;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.TwoPhaseCommitTransaction;
import com.scalar.db.common.error.CoreError;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.transaction.AbortException;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.PreparationException;
import com.scalar.db.exception.transaction.RollbackException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.exception.transaction.TransactionNotFoundException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.exception.transaction.ValidationException;
import com.scalar.db.util.ActiveExpiringMap;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ActiveTransactionManagedTwoPhaseCommitTransactionManager
    extends AbstractTwoPhaseCommitTransactionManager {

  private static final long TRANSACTION_EXPIRATION_INTERVAL_MILLIS = 1000;

  private static final Logger logger =
      LoggerFactory.getLogger(AbstractTwoPhaseCommitTransactionManager.class);

  private final ActiveExpiringMap<String, ActiveTransaction> activeTransactions;

  private final AtomicReference<BiConsumer<String, TwoPhaseCommitTransaction>>
      transactionExpirationHandler =
          new AtomicReference<>(
              (id, t) -> {
                try {
                  t.rollback();
                } catch (Exception e) {
                  logger.warn("Rollback failed. Transaction ID: {}", id, e);
                }
              });

  public ActiveTransactionManagedTwoPhaseCommitTransactionManager(DatabaseConfig config) {
    super(config);
    activeTransactions =
        new ActiveExpiringMap<>(
            config.getActiveTransactionManagementExpirationTimeMillis(),
            TRANSACTION_EXPIRATION_INTERVAL_MILLIS,
            (id, t) -> {
              logger.warn("The transaction is expired. Transaction ID: {}", id);
              transactionExpirationHandler.get().accept(id, t);
            });
  }

  public void setTransactionExpirationHandler(
      BiConsumer<String, TwoPhaseCommitTransaction> handler) {
    transactionExpirationHandler.set(handler);
  }

  private void add(ActiveTransaction transaction) throws TransactionException {
    if (activeTransactions.putIfAbsent(transaction.getId(), transaction).isPresent()) {
      transaction.rollback();
      throw new TransactionException(
          CoreError.TRANSACTION_ALREADY_EXISTS.buildMessage(), transaction.getId());
    }
  }

  private void remove(String transactionId) {
    activeTransactions.remove(transactionId);
  }

  protected boolean isTransactionActive(String transactionId) {
    return activeTransactions.containsKey(transactionId);
  }

  @Override
  public TwoPhaseCommitTransaction resume(String txId) throws TransactionNotFoundException {
    return activeTransactions
        .get(txId)
        .orElseThrow(
            () ->
                new TransactionNotFoundException(
                    CoreError.TRANSACTION_NOT_FOUND.buildMessage(), txId));
  }

  @Override
  protected TwoPhaseCommitTransaction decorate(TwoPhaseCommitTransaction transaction)
      throws TransactionException {
    return new ActiveTransaction(super.decorate(transaction));
  }

  private class ActiveTransaction extends DecoratedTwoPhaseCommitTransaction {

    @SuppressFBWarnings("EI_EXPOSE_REP2")
    private ActiveTransaction(TwoPhaseCommitTransaction transaction) throws TransactionException {
      super(transaction);
      add(this);
    }

    // For the SpotBugs warning CT_CONSTRUCTOR_THROW
    @Override
    protected final void finalize() {}

    @Override
    public synchronized Optional<Result> get(Get get) throws CrudException {
      return super.get(get);
    }

    @Override
    public synchronized List<Result> scan(Scan scan) throws CrudException {
      return super.scan(scan);
    }

    @Override
    public synchronized void put(Put put) throws CrudException {
      super.put(put);
    }

    @Override
    public synchronized void put(List<Put> puts) throws CrudException {
      super.put(puts);
    }

    @Override
    public synchronized void delete(Delete delete) throws CrudException {
      super.delete(delete);
    }

    @Override
    public synchronized void delete(List<Delete> deletes) throws CrudException {
      super.delete(deletes);
    }

    @Override
    public synchronized void mutate(List<? extends Mutation> mutations) throws CrudException {
      super.mutate(mutations);
    }

    @Override
    public synchronized void prepare() throws PreparationException {
      super.prepare();
    }

    @Override
    public synchronized void validate() throws ValidationException {
      super.validate();
    }

    @Override
    public synchronized void commit() throws CommitException, UnknownTransactionStatusException {
      super.commit();
      remove(getId());
    }

    @Override
    public synchronized void rollback() throws RollbackException {
      try {
        super.rollback();
      } finally {
        remove(getId());
      }
    }

    @Override
    public void abort() throws AbortException {
      try {
        super.abort();
      } finally {
        remove(getId());
      }
    }
  }
}
