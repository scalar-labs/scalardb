package com.scalar.db.common;

import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.Get;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.RollbackException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.exception.transaction.TransactionNotFoundException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.util.ActiveExpiringMap;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.List;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ActiveTransactionManagedTransactionManager
    extends AbstractDistributedTransactionManager {

  private static final long TRANSACTION_EXPIRATION_INTERVAL_MILLIS = 1000;

  private static final Logger logger =
      LoggerFactory.getLogger(ActiveTransactionManagedTransactionManager.class);

  private final ActiveExpiringMap<String, DistributedTransaction> activeTransactions;

  public ActiveTransactionManagedTransactionManager(DatabaseConfig config) {
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

  private void add(DistributedTransaction transaction) throws TransactionException {
    if (activeTransactions.putIfAbsent(transaction.getId(), transaction) != null) {
      transaction.rollback();
      throw new TransactionException(
          "The transaction already exists. transactionId: " + transaction.getId());
    }
  }

  private void remove(String transactionId) {
    activeTransactions.remove(transactionId);
  }

  @Override
  public DistributedTransaction resume(String txId) throws TransactionNotFoundException {
    return activeTransactions
        .get(txId)
        .orElseThrow(
            () ->
                new TransactionNotFoundException(
                    "A transaction associated with the specified transaction ID is not found. "
                        + "It might have been expired. transactionId: "
                        + txId));
  }

  @Override
  protected DistributedTransaction activate(DistributedTransaction transaction)
      throws TransactionException {
    return new ActiveTransaction(super.activate(transaction));
  }

  private class ActiveTransaction extends AbstractDistributedTransaction
      implements WrappedDistributedTransaction {

    private final DistributedTransaction transaction;

    @SuppressFBWarnings("EI_EXPOSE_REP2")
    private ActiveTransaction(DistributedTransaction transaction) throws TransactionException {
      this.transaction = transaction;
      add(this);
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
    public void commit() throws CommitException, UnknownTransactionStatusException {
      transaction.commit();
      remove(getId());
    }

    @Override
    public void rollback() throws RollbackException {
      try {
        transaction.rollback();
      } finally {
        remove(getId());
      }
    }

    @Override
    public DistributedTransaction getOriginalTransaction() {
      if (transaction instanceof WrappedDistributedTransaction) {
        return ((WrappedDistributedTransaction) transaction).getOriginalTransaction();
      }
      return transaction;
    }
  }
}
